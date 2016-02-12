package com.basho.riak.client.core;

import java.util.Iterator;
import java.util.List;

/**
 * @author Alex Moore <amoore at basho dot com>
 * @param <T> The type the operation returns
 * @param <U> The protocol type returned
 * @param <S> Query info type
 * @since 2.0.5
 */
public abstract class StreamingFutureOperation<T, U, S>
        extends FutureOperation<Void,U,S>
        implements StreamingRiakFuture<T,S>
{
    public synchronized void setResponse(RiakMessage rawResponse)
    {
        stateCheck(State.CREATED, State.WRITTEN, State.RETRY);
        U decodedMessage = decode(rawResponse);

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

    protected Void convert(List<U> rawResponse)
    {
        // Nothing added to the rawResponse list, nothing to convert;
        return null;
    }

    abstract protected Void processStreamingChunk(U rawResponseChunk);
}
