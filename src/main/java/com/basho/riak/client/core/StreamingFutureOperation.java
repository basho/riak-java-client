package com.basho.riak.client.core;

/**
 * @author Alex Moore <amoore at basho dot com>
 * @param <ReturnType> The type returned by the streaming and non-streaming operation versions
 * @param <ResponseType> The protocol type returned
 * @param <QueryInfoType> Query info type
 * @since 2.0.7
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

    public synchronized void setResponse(RiakMessage rawResponse)
    {
        if(!streamResults)
        {
            super.setResponse(rawResponse);
            return;
        }

        stateCheck(State.CREATED, State.WRITTEN, State.RETRY);
        ResponseType decodedMessage = decode(rawResponse);

        processStreamingChunk(decodedMessage);

        exception = null;

        if (done(decodedMessage))
        {
            remainingTries--;
            if (retrier != null)
            {
                retrier.operationComplete(this, remainingTries);
            }
            state = State.COMPLETE;
            latch.countDown();
            fireListeners();
        }
    }

    abstract protected void processStreamingChunk(ResponseType rawResponseChunk);
}
