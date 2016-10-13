package com.basho.riak.client.api.commands;

import com.basho.riak.client.core.StreamingRiakFuture;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.function.Function;

public class ChunkedQueueIterator<FinalT, ChunkT extends Iterable<CoreT>, CoreT> implements Iterator<FinalT>
{
    private final int timeout;
    private final StreamingRiakFuture<ChunkT, ?> coreFuture;
    private final TransferQueue<ChunkT> chunkQueue;
    private final Function<CoreT, FinalT> createNext;
    private final Function<ChunkT, Iterator<CoreT>> getNextIterator;

    private Iterator<CoreT> currentIterator = null;

    public ChunkedQueueIterator(StreamingRiakFuture<ChunkT, ?> coreFuture,
                                int pollTimeout,
                                Function<CoreT, FinalT> createNext,
                                Function<ChunkT, Iterator<CoreT>> getNextIterator)
    {
        this.timeout = pollTimeout;
        this.coreFuture = coreFuture;
        this.chunkQueue = coreFuture.getResultsQueue();
        this.createNext = createNext;
        this.getNextIterator = getNextIterator;
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

    @Override
    public synchronized FinalT next()
    {
        if(!hasNext())
        {
            return null;
        }

        if(!currentIteratorHasNext())
        {
            try
            {
                loadNextChunkIterator();
            }
            catch (InterruptedException e)
            {
                // Catch InterruptedException at this level so that the chunk is fully loaded and
                // ready to run next(), etc again in the future.
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            if(!hasNext())
            {
                return null;
            }
        }

        return createNext.apply(currentIterator.next());
    }

    private void loadNextChunkIterator() throws InterruptedException
    {
        this.currentIterator = null;
        boolean populatedChunkLoaded = false;

        while(!populatedChunkLoaded && possibleChunksRemaining())
        {
            final ChunkT nextChunk = chunkQueue.poll(timeout, TimeUnit.MILLISECONDS);
            if(nextChunk != null)
            {
                this.currentIterator = getNextIterator.apply(nextChunk);
                populatedChunkLoaded = currentIteratorHasNext();
            }
        }
    }
}
