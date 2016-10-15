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
        loadNextChunkIterator();
    }

    @Override
    public boolean hasNext()
    {
        return currentIteratorHasNext() || possibleChunksRemaining();
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
    public synchronized FinalT next()
    {
        if(!hasNext())
        {
            return null;
        }

        if(!currentIteratorHasNext())
        {
            loadNextChunkIterator();

            if(!hasNext())
            {
                return null;
            }
        }

        return createNext.apply(currentIterator.next());
    }

    private void loadNextChunkIterator()
    {
        this.currentIterator = null;
        boolean populatedChunkLoaded = false;

        try
        {
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
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void loadContinuation(ChunkT nextChunk)
    {
        final BinaryValue fetchedContinuation = getContinuationFn.apply(nextChunk);
        if(this.continuation == null && fetchedContinuation != null)
        {
            this.continuation = fetchedContinuation;
        }
    }
}
