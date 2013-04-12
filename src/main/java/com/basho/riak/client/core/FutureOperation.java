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


import com.basho.riak.client.FutureListener;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public abstract class FutureOperation<T> implements Future<T>
{
    private enum State { CREATED, WRITTEN, COMPLETE, CANCELLED }
    private final Logger logger = LoggerFactory.getLogger(FutureOperation.class);
    private final List<FutureListener> futureListeners = 
        Collections.synchronizedList(new LinkedList<FutureListener>());
    private final List<Protocol> protocolPreflist = new LinkedList<>(Arrays.asList(Protocol.values()));
    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile OperationRetrier retrier;
    private volatile int numRetries = 0;
    private volatile RiakResponse rawResponse;
    private volatile Throwable exception;
    private volatile T converted;
    private volatile State state = State.CREATED;
    private RiakNode lastNode;
    
    public final void addListener(FutureListener<T> listener)
    {
        if (state == State.COMPLETE)
        {
            listener.operationComplete(this);
        }
        else
        {
            futureListeners.add(listener);
        }
    }

    public final void removeListener(FutureListener<T> listener)
    {
        futureListeners.remove(listener);
    }
    
    private void notifyListeners()
    {
        synchronized(futureListeners)
        {
            for (Iterator<FutureListener> it = futureListeners.iterator(); it.hasNext();)
            {
                FutureListener<T> listener = it.next();
                listener.operationComplete(this);
            }
        }
    }
    
    final synchronized void setRetrier(OperationRetrier retrier, int numRetries)
    {
        stateCheck(State.CREATED);
        this.retrier = retrier;
        this.numRetries = numRetries;
    }

    final RiakNode getLastNode()
    {
        return lastNode;
    }
    
    final void setLastNode(RiakNode node)
    {
        this.lastNode = node;
    }
    
    final List<Protocol> getProtocolPreflist()
    {
        return Collections.unmodifiableList(protocolPreflist);
    }

    protected final synchronized void supportedProtocol(Protocol... protocols)
    {
        stateCheck(State.CREATED);
        protocolPreflist.clear();
        protocolPreflist.addAll(Arrays.asList(protocols));
    }
    
    synchronized void setResponse(RiakResponse rawResponse)
    {
        this.rawResponse = rawResponse;
        exception = null;
        state = State.COMPLETE;
        latch.countDown();
        if (retrier != null)
        {
            retrier.operationComplete(this, numRetries);
        }
        notifyListeners();
    }

    synchronized void setException(Throwable t)
    {
        this.exception = t;
        state = State.COMPLETE;
        
        if (numRetries == 0)
        {
            latch.countDown();
        }
        else
        {
            numRetries--;
        }
        
        if (retrier != null)
        {
            retrier.operationFailed(this, numRetries);
        }
    }

    public synchronized final Object channelMessage(Protocol p)
    {
        stateCheck(State.CREATED, State.COMPLETE);
        Object message = createChannelMessage(p);
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
    public final T get() throws InterruptedException, ExecutionException
    {
        stateCheck(State.WRITTEN, State.COMPLETE);
        latch.await();
        
        if (exception != null)
        {
            throw new ExecutionException(exception);
        }
        
        if (null == converted)
        {
            converted = convert(rawResponse);
        }
        
        return converted;
    }

    @Override
    public final T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
        stateCheck(State.WRITTEN, State.COMPLETE);
        boolean succeed = latch.await(timeout, unit);
        
        if (!succeed)
        {
            throw new TimeoutException();
        }
        
        if (exception != null)
        {
            throw new ExecutionException(exception);
        }
        
        if (null == converted)
        {
            converted = convert(rawResponse);
        }
        
        return converted;
    }
    
    
    private void stateCheck(State... allowedStates)
    {
        if (Arrays.binarySearch(allowedStates, state) < 0)
        {
            logger.debug("IllegalStateException; required: {} current: {} ",
                         Arrays.toString(allowedStates), state);
            throw new IllegalStateException("required: " 
                + Arrays.toString(allowedStates) 
                + " current: " + state );
        }
    }
    
    abstract protected T convert(RiakResponse rawResponse);
    abstract protected Object createChannelMessage(Protocol p);
    
    
    
}
