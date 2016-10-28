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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.function.Function;

public class ChunkedResponseIterator<FinalT, ChunkT extends Iterable<CoreT>, CoreT> implements Iterator<FinalT>
{
    private final int timeout;
    private volatile BinaryValue continuation = null;
    private final StreamingRiakFuture<ChunkT, ?> coreFuture;
    private final TransferQueue<ChunkT> chunkQueue;
    private final Function<CoreT, FinalT> createNext;
    private final Function<ChunkT, Iterator<CoreT>> getNextIterator;
    private final Function<ChunkT, BinaryValue> getContinuationFn;

    private Iterator<CoreT> currentIterator = null;

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


        // Check & clear interrupted flag so we don't get an
        // InterruptedException every time if the user
        // doesn't clear it / deal with it.
        boolean interrupted = Thread.interrupted();
        try
        {
            boolean lastLoadInterrupted;
            do
            {
                lastLoadInterrupted = false;
                try
                {
                    tryLoadNextChunkIterator();
                }
                catch (InterruptedException ex)
                {
                    interrupted = true;
                    lastLoadInterrupted = true;
                }
            } while (lastLoadInterrupted);
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

    public boolean hasContinuation()
    {
        return continuation != null || possibleChunksRemaining();
    }

    public BinaryValue getContinuation()
    {
        return continuation;
    }

    @Override
    public FinalT next()
    {
        return createNext.apply(currentIterator.next());
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
