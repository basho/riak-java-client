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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public abstract class FutureOperation<T> implements RiakFuture<T>
{
    private enum State { CREATED, WRITTEN, RETRY, COMPLETE, CANCELLED }
    private final Logger logger = LoggerFactory.getLogger(FutureOperation.class);
    private final List<Protocol> protocolPreflist = new LinkedList<Protocol>(Arrays.asList(Protocol.values()));
    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile OperationRetrier retrier;
    private volatile int remainingTries = 1;
    private volatile RiakResponse rawResponse;
    private volatile Throwable exception;
    private volatile T converted;
    private volatile State state = State.CREATED;
    private volatile RiakNode lastNode;
    private volatile boolean noConversion = false;
    
    
    final synchronized void setRetrier(OperationRetrier retrier, int numTries)
    {
        stateCheck(State.CREATED);
        this.retrier = retrier;
        this.remainingTries = numTries;
    }

    /**
     * No conversion will be done to the raw response from Riak.
     * <p>
     * This option is included with people interested in building their own
     * clients on top of the core in mind.
     * </p>
     * <p>
     * If set to {@code true} then {@link #convert(com.basho.riak.client.core.RiakResponse) }
     * will not be called and both {@link #get() } and {@link #get(long, java.util.concurrent.TimeUnit) }
     * will throw an {@code IllegalStateException}. 
     * </p>
     * The {@link RiakResponse} can be retrieved via {@link #getRiakResponse()} or 
     * {@link #getRiakResponse(long, java.util.concurrent.TimeUnit)}.
     * 
     * @param noConversion 
     */
    public final void setNoConversion(boolean noConversion)
    {
        // This is really a future-proofing. Right now the conversion is actually 
        // done by the caller's thread in the get() methods, but if that is moved
        // to a netty thread then this will be useful.
        this.noConversion = noConversion;
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

    protected final synchronized void supportedProtocols(Protocol... protocols)
    {
        stateCheck(State.CREATED);
        protocolPreflist.clear();
        protocolPreflist.addAll(Arrays.asList(protocols));
    }
    
    synchronized final void setResponse(RiakResponse rawResponse)
    {
        remainingTries--;
        this.rawResponse = rawResponse;
        exception = null;
        state = State.COMPLETE;
        latch.countDown();
        if (retrier != null)
        {
            retrier.operationComplete(this, remainingTries);
        }
    }

    synchronized final void setException(Throwable t)
    {
        this.exception = t;
        
        remainingTries--;
        if (remainingTries == 0)
        {
            state = State.COMPLETE;
            latch.countDown();
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

    public synchronized final Object channelMessage(Protocol p)
    {
        stateCheck(State.CREATED, State.RETRY);
        Object message = createChannelMessage(p);
        state = State.WRITTEN;
        return message;
    }
    
    public final RiakResponse getRiakResponse() throws InterruptedException, ExecutionException
    {
        latch.await();
        
        if (exception != null)
        {
            throw new ExecutionException(exception);
        }
        
        return rawResponse;
    }
    
    public final RiakResponse getRiakResponse(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException, ExecutionException
    {
        boolean succeed = latch.await(timeout, unit);
        
        if (!succeed)
        {
            throw new TimeoutException();
        }
        
        if (exception != null)
        {
            throw new ExecutionException(exception);
        }
        
        return rawResponse;
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
        if (noConversion)
        {
            throw new IllegalStateException("noConversion set to true");
        }
        
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
        if (noConversion)
        {
            throw new IllegalStateException("noConversion set to true");
        }
        
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
    
    abstract protected T convert(RiakResponse rawResponse) throws ExecutionException;
    abstract protected Object createChannelMessage(Protocol p);
    
    
}
