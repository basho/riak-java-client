package com.basho.riak.client.core;

/**
 * @author Alex Moore <amoore at basho dot com>
 * @param <SyncReturnType> The type the non-streaming operation returns
 * @param <ResponseType> The protocol type returned
 * @param <QueryInfoType> Query info type
 * @param <StreamingReturnType> The type the streaming operation queue returns
 * @since 2.0.7
 */
public abstract class StreamingFutureOperation<SyncReturnType, StreamingReturnType, ResponseType, QueryInfoType>
        extends FutureOperation<SyncReturnType, ResponseType, QueryInfoType>
        implements StreamingRiakFuture<SyncReturnType, StreamingReturnType, QueryInfoType>
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
