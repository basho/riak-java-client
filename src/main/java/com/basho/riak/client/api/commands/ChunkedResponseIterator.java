/*
 * Copyright 2016 Basho Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.basho.riak.client.api.commands;

import com.basho.riak.client.core.StreamingRiakFuture;
import com.basho.riak.client.core.util.BinaryValue;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.function.Function;

/**
 * Transforms a stream of response chunks to a Iterable of response items.
 *
 * When iterating over this class's {@link Iterator} this class will lazily walk
 * through the response chunks's iterators and convert the items.
 * It will also wait for more response chunks if none are available.
 *
 * Since this class polls for new "streaming" data, it is advisable
 * to check {@link Thread#isInterrupted()} while using this class's
 * {@link Iterator} in environments where thread interrupts must be obeyed.
 *
 * @param <FinalT> The final converted type that this class exposes as part of its iterator.
 * @param <ChunkT> The type of the response chunks, contains an Iterable&lt;<b>CoreT</b>&gt;
 * @param <CoreT> The raw response type, will get converted to <b>FinalT</b>.
 * @author Alex Moore <amoore at basho.com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.1.0
 */
public class ChunkedResponseIterator<FinalT, ChunkT extends Iterable<CoreT>, CoreT> implements Iterator<FinalT>
{
    private final int timeout;
    private volatile BinaryValue continuation = null;
    private final StreamingRiakFuture<ChunkT, ?> coreFuture;
    private final TransferQueue<ChunkT> chunkQueue;
    private final Function<CoreT, FinalT> createNext;
    private final Function<ChunkT, Iterator<CoreT>> getNextIterator;
    private final Function<ChunkT, BinaryValue> getContinuationFn;

    protected Iterator<CoreT> currentIterator = null;

    public ChunkedResponseIterator(StreamingRiakFuture<ChunkT, ?> coreFuture,
                                   int pollTimeout,
                                   Function<CoreT, FinalT> createNextFn,
                                   Function<ChunkT, Iterator<CoreT>> getNextIteratorFn)
    {
        this(coreFuture, pollTimeout, createNextFn, getNextIteratorFn, (x) -> null);
    }

    public ChunkedResponseIterator(StreamingRiakFuture<ChunkT, ?> coreFuture,
                                   int pollTimeout,
                                   Function<CoreT, FinalT> createNextFn,
                                   Function<ChunkT, Iterator<CoreT>> getNextIteratorFn,
                                   Function<ChunkT, BinaryValue> getContinuationFn)
    {
        this.timeout = pollTimeout;
        this.coreFuture = coreFuture;
        this.chunkQueue = coreFuture.getResultsQueue();
        this.createNext = createNextFn;
        this.getNextIterator = getNextIteratorFn;
        this.getContinuationFn = getContinuationFn;

        // to kick of initial loading
        hasNext();
    }

    /**
     * Returns {@code true} if the iteration has more elements.
     * (In other words, returns {@code true} if {@link #next} would
     * return an element rather than throwing an exception.)
     *
     * This method will block and wait for more data if none is immediately available.
     *
     * <b>Riak Java Client Note:</b> Since this class polls for
     * new "streaming" data, it is advisable to check {@link Thread#isInterrupted()}
     * in environments where thread interrupts must be obeyed.
     *
     * @return {@code true} if the iteration has more elements
     */
    @Override
    public boolean hasNext()
    {
        // Check & clear interrupted flag so we don't get an
        // InterruptedException every time if the user
        // doesn't clear it / deal with it.
        boolean interrupted = Thread.interrupted();
        Boolean dataLoaded = null;

        try
        {
            while (!currentIteratorHasNext() && dataLoaded == null)
            {
                try
                {
                    dataLoaded = tryLoadNextChunkIterator();
                }
                catch (InterruptedException ex)
                {
                    interrupted = true;
                }
            }
            return currentIteratorHasNext();
        }
        finally
        {
            if (interrupted)
            {
                // Reset interrupted flag if we came in with it
                // or we were interrupted while waiting.
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean currentIteratorHasNext()
    {
        return currentIterator != null && currentIterator.hasNext();
    }

    private boolean possibleChunksRemaining()
    {
        // Chunks may remain if :
        // Core Operation Not Done OR items still in chunk Queue
        return !coreFuture.isDone() || !chunkQueue.isEmpty();
    }

    /**
     * Returns whether this response contains a continuation.
     * Only run this once the operation is complete, otherwise it will return true as it's
     * @return Whether this response has a continuation.
     */
    public boolean hasContinuation()
    {
        return continuation != null || possibleChunksRemaining();
    }

    /**
     * Returns the current value of the continuation.
     * Only run this once the operation is complete, or else you will get a null value.
     * @return The continuation value (if any).
     */
    public BinaryValue getContinuation()
    {
        return continuation;
    }

    /**
     * Returns the next element in the iteration.
     * This method will block and wait for more data if none is immediately available.
     *
     * <b>Riak Java Client Note:</b> Since this class polls for
     * new "streaming" data, it is advisable to check {@link Thread#isInterrupted()}
     * in environments where thread interrupts must be obeyed.
     *
     * @return the next element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    @Override
    public FinalT next()
    {
        if (hasNext())
        {
            return createNext.apply(currentIterator.next());
        }

        throw new NoSuchElementException();
    }

    private boolean tryLoadNextChunkIterator() throws InterruptedException
    {
        this.currentIterator = null;
        boolean populatedChunkLoaded = false;

        while (!populatedChunkLoaded && possibleChunksRemaining())
        {
            final ChunkT nextChunk = chunkQueue.poll(timeout, TimeUnit.MILLISECONDS);

            if (nextChunk != null)
            {
                this.currentIterator = getNextIterator.apply(nextChunk);
                populatedChunkLoaded = currentIteratorHasNext();

                loadContinuation(nextChunk);
            }
        }

        return populatedChunkLoaded;
    }

    private void loadContinuation(ChunkT nextChunk)
    {
        final BinaryValue fetchedContinuation = getContinuationFn.apply(nextChunk);
        if (this.continuation == null && fetchedContinuation != null)
        {
            this.continuation = fetchedContinuation;
        }
    }
}
