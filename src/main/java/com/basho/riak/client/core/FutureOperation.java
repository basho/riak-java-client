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

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Brian Roach <roach at basho dot com>
 * @param <T> The type the operation returns
 * @param <U> The protocol type returned
 * @param <S> Query info type
 * @since 2.0
 *
 *  State Transition Diagram
 *
 *                            New Operation
 *                                  |
 *                                  |
 *                                  |
 *                                  |
 *        +-------------------------|---------Cancel-------------+
 *        |                         |                            |
 *        |                   +-----v-----+                      |
 *        |                   |           |                      |
 *        +------Cancel-------+  CREATED  +----Exception------+  |
 *        |                   |           |                   |  |
 *        |                   +-----------+                   |  |
 *        |                         |                     +---v-----+
 *        |                         +<---Retries Left-----+         +---+
 *        |                         |                     |  RETRY  |   |
 *        |                     Write Data                |         |   |
 *        |                         |                     +---^-----+   |
 * +------v--- --+            +-----v-----+                   |         |
 * |             |            |           +----Exception------+         |
 * |  CANCELLED  <----Cancel--+  WRITTEN  |                             |
 * |             |            |           |                             |
 * +-------------+            +-----------+                             |
 *                                  |                                   |
 *                                Read OK                               |
 *                             Response Done                            |
 *                                  |                                   |
 *                          +-------v--------+                          |
 *                          |                |                          |
 *                          |  CLEANUP_WAIT  <------No Retries Left-----+
 *                          |                |
 *                          +----------------+
 *                                  |
 *                     **Caller Returns Connection**
 *                              setComplete()
 *                                  |
 *                            +-----v------+
 *                            |            |
 *                            |  COMPLETE  |
 *                            |            |
 *                            +------------+
 *
 */

public abstract class FutureOperation<T, U, S> implements RiakFuture<T,S>
{
    private enum State
    {
        CREATED, WRITTEN, RETRY, COMPLETE, CANCELLED,
        CLEANUP_WAIT
    }

    private final Logger logger = LoggerFactory.getLogger(FutureOperation.class);
    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile OperationRetrier retrier;
    private volatile int remainingTries = 1;
    private volatile List<U> rawResponses = new LinkedList<>();
    private volatile Throwable exception;
    private volatile T converted;
    private volatile State state = State.CREATED;
    private volatile RiakNode lastNode;

    private final ReentrantLock listenersLock = new ReentrantLock();
    private final HashSet<RiakFutureListener<T,S>> listeners = new HashSet<>();
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

    private void fireListeners()
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
    public synchronized final void setResponse(RiakMessage rawResponse)
    {
        stateCheck(State.CREATED, State.WRITTEN, State.RETRY);
        U decodedMessage = decode(rawResponse);

        processMessage(decodedMessage);

        exception = null;
        if (done(decodedMessage))
        {
            logger.debug("Setting to Cleanup Wait State");
            remainingTries--;
            if (retrier != null)
            {
                retrier.operationComplete(this, remainingTries);
            }
            state = State.CLEANUP_WAIT;
        }
    }

    protected void processMessage(U decodedMessage)
    {
        processBatchMessage(decodedMessage);
    }

    private void processBatchMessage(U decodedMessage)
    {
        this.rawResponses.add(decodedMessage);
    }

    public synchronized final void setComplete()
    {
        logger.debug("Setting Complete on future");
        stateCheck(State.CLEANUP_WAIT);
        state = State.COMPLETE;
        latch.countDown();
        fireListeners();
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
            // Connection should be returned before calling
            state = State.CLEANUP_WAIT;
            setComplete();
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
        final Object message = createChannelMessage();
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
        return state == State.COMPLETE || state == State.CLEANUP_WAIT;
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

        throwExceptionIfSet();

        if (null == converted)
        {
            tryConvertResponse();
        }

        return converted;
    }

    private void throwExceptionIfSet() throws ExecutionException
    {
        if (exception != null)
        {
            throw new ExecutionException(exception);
        }
    }

    private void tryConvertResponse() throws ExecutionException
    {
        try
        {
            converted = convert(rawResponses);
        }
        catch (IllegalArgumentException ex)
        {
            exception = ex;
            throwExceptionIfSet();
        }
    }

    @Override
    public final T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
        boolean succeed = latch.await(timeout, unit);

        if (!succeed)
        {
            throw new TimeoutException();
        }

        throwExceptionIfSet();

        if (null == converted)
        {
            tryConvertResponse();
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
    public final boolean await(long timeout, TimeUnit unit) throws InterruptedException
    {
        return latch.await(timeout, unit);
    }

    protected U checkAndGetSingleResponse(List<U> responses)
    {
        if (responses.size() > 1)
        {
            LoggerFactory.getLogger(this.getClass()).error("Received {} responses when only one was expected.",
                                                           responses.size());
        }

        return responses.get(0);
    }

    private void stateCheck(State... allowedStates)
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
