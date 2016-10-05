package com.basho.riak.client.core;

/**
 * @author Alex Moore <amoore at basho dot com>
 * @param <ReturnType> The type returned by the streaming and non-streaming operation versions
 * @param <ResponseType> The protocol type returned
 * @param <QueryInfoType> Query info type
 * @since 2.1.0
 */
public abstract class StreamingFutureOperation<ReturnType, ResponseType, QueryInfoType>
        extends FutureOperation<ReturnType, ResponseType, QueryInfoType>
        implements StreamingRiakFuture<ReturnType, QueryInfoType>
{
    private boolean streamResults;

    protected StreamingFutureOperation(boolean streamResults)
    {
        this.streamResults = streamResults;
    }

    @Override
    protected void processMessage(ResponseType decodedMessage)
    {
        if(!streamResults)
        {
            super.processMessage(decodedMessage);
            return;
        }

        processStreamingChunk(decodedMessage);
    }

    abstract protected void processStreamingChunk(ResponseType rawResponseChunk);
}
