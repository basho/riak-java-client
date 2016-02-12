/*
 * Copyright 2013 Basho Technologies Inc.
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
package com.basho.riak.client.core;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Brian Roach <roach at basho dot com>
 * @param <T> The type the operation returns
 * @param <U> The protocol type returned 
 * @param <S> Query info type
 * @since 2.0
 */
public abstract class FutureOperation<T, U, S> implements RiakFuture<T,S>
{

    protected enum State
    {
        CREATED, WRITTEN, RETRY, COMPLETE, CANCELLED
    }

    private final Logger logger = LoggerFactory.getLogger(FutureOperation.class);
    protected final CountDownLatch latch = new CountDownLatch(1);
    protected volatile OperationRetrier retrier;
    protected volatile int remainingTries = 1;
    protected volatile LinkedList<U> rawResponses = new LinkedList<U>();
    protected volatile Throwable exception;
    private volatile T converted;
    protected volatile State state = State.CREATED;
    private volatile RiakNode lastNode;

    private final ReentrantLock listenersLock = new ReentrantLock();
    private final HashSet<RiakFutureListener<T,S>> listeners =
        new HashSet<RiakFutureListener<T,S>>();
    private volatile boolean listenersFired = false;

    @Override
    public void addListener(RiakFutureListener<T,S> listener)
    {

        boolean fireNow = false;
        listenersLock.lock();
        try
        {
            if (listenersFired)
            {
                fireNow = true;
            }
            else
            {
                listeners.add(listener);
            }
        }
        finally
        {
            listenersLock.unlock();
        }

        // the future has already been completed, fire on caller's thread
        if (fireNow)
        {
            listener.handle(this);
        }

    }

    @Override
    public void removeListener(RiakFutureListener<T,S> listener)
    {
        listenersLock.lock();
        try
        {
            if (!listenersFired)
            {
                listeners.remove(listener);
            } // else, we don't care, they've already been fired
        }
        finally
        {
            listenersLock.unlock();
        }
    }

    protected void fireListeners()
    {

        boolean fireNow = false;
        listenersLock.lock();
        try
        {
            if (!listenersFired)
            {
                fireNow = true;
                listenersFired = true;
            }
        }
        finally
        {
            listenersLock.unlock();
        }

        if (fireNow)
        {
            for (RiakFutureListener<T,S> listener : listeners)
            {
                listener.handle(this);
            }
        }

    }

    final synchronized void setRetrier(OperationRetrier retrier, int numTries)
    {
        stateCheck(State.CREATED);
        this.retrier = retrier;
        this.remainingTries = numTries;
    }

    final RiakNode getLastNode()
    {
        return lastNode;
    }

    final void setLastNode(RiakNode node)
    {
        this.lastNode = node;
    }

    // Exposed for testing.
    public synchronized void setResponse(RiakMessage rawResponse)
    {
        stateCheck(State.CREATED, State.WRITTEN, State.RETRY);
        U decodedMessage = decode(rawResponse);
        this.rawResponses.add(decodedMessage);
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

    /**
     * Detect when the streaming operation is finished
     *
     * @param message raw message
     * @return returns true if this is the last message in the streaming operation
     */
    protected boolean done(U message)
    {
        return true;
    }

    synchronized final void setException(Throwable t)
    {
        stateCheck(State.CREATED, State.WRITTEN, State.RETRY);
        this.exception = t;

        remainingTries--;
        if (remainingTries == 0)
        {
            state = State.COMPLETE;
            latch.countDown();
            fireListeners();
        }
        else
        {
            state = State.RETRY;
        }

        if (retrier != null)
        {
            retrier.operationFailed(this, remainingTries);
        }

    }

    public synchronized final Object channelMessage()
    {
        Object message = createChannelMessage();
        state = State.WRITTEN;
        return message;
    }

    @Override
    public final boolean cancel(boolean mayInterruptIfRunning)
    {
        return false;
    }

    @Override
    public final boolean isCancelled()
    {
        return state == State.CANCELLED;
    }

    @Override
    public final boolean isDone()
    {
        return state == State.COMPLETE;
    }

    @Override
    public final boolean isSuccess()
    {
        return (isDone() && exception == null);
    }
    
    @Override
    public final Throwable cause()
    {
        if (isSuccess())
        {
            return null;
        }
        else
        {
            return exception;
        }
    }
    
    @Override
    public final T get() throws InterruptedException, ExecutionException
    {
        latch.await();

        if (exception != null)
        {
            throw new ExecutionException(exception);
        }
        else if (null == converted)
        {
            converted = convert(rawResponses);
            
        }

        return converted;
    }

    @Override
    public final T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
        boolean succeed = latch.await(timeout, unit);

        if (!succeed)
        {
            throw new TimeoutException();
        }
        else if (exception != null)
        {
            throw new ExecutionException(exception);
        }
        else if (null == converted)
        {
            converted = convert(rawResponses);
        }
        
        return converted;
    }

    @Override
    public final T getNow()
    {
        if (latch.getCount() < 1)
        {
            if (null == converted)
            {
                converted = convert(rawResponses);
            }
            
            return converted;
        }
        else
        {
            return null;
        }
    }
    
    @Override
    public final void await() throws InterruptedException
    {
        latch.await();
    }

    @Override
    public final void await(long timeout, TimeUnit unit) throws InterruptedException
    {
        latch.await(timeout, unit);
    }
    
    
    protected void stateCheck(State... allowedStates)
    {
        if (Arrays.binarySearch(allowedStates, state) < 0)
        {
            logger.debug("IllegalStateException; required: {} current: {} ",
                Arrays.toString(allowedStates), state);
            throw new IllegalStateException("required: "
                + Arrays.toString(allowedStates)
                + " current: " + state);
        }
    }

    abstract protected T convert(List<U> rawResponse);

    abstract protected RiakMessage createChannelMessage();

    abstract protected U decode(RiakMessage rawMessage);

    @Override
    abstract public S getQueryInfo();

}
