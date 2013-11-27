/*
 * Copyright 2013 Basho Technologies Inc
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

import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class AbstractRiakFuture<V> implements RiakFuture<V>
{

    private final HashSet<RiakFutureListener<V>> listeners = new HashSet<RiakFutureListener<V>>();
    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile boolean listenersFired = false;
    private volatile Throwable exception;
    private volatile V result;

    protected synchronized boolean set(V result)
    {
        if (!isDone())
        {
            this.result = result;
            latch.countDown();
            fireListeners();
            return true;
        }
        return false;
    }

    protected synchronized boolean setException(Throwable t)
    {
        if (!isDone())
        {
            this.exception = t;
            latch.countDown();
            fireListeners();
            return true;
        }
        return false;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning)
    {
        return false;
    }

    private synchronized V internalGet() throws ExecutionException
    {
        if (result != null)
        {
            return result;
        }

        throw new ExecutionException(exception);
    }

    @Override
    public V get() throws InterruptedException, ExecutionException
    {
        latch.await();

        return internalGet();

    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
        if (!latch.await(timeout, unit))
        {
            throw new TimeoutException();
        }

        return internalGet();
    }

    @Override
    public boolean isCancelled()
    {
        return false;
    }

    @Override
    public synchronized boolean isDone()
    {
        return (result != null) || (exception != null);
    }

    @Override
    public synchronized void addListener(RiakFutureListener<V> listener)
    {

        boolean fireNow = false;
        if (listenersFired)
        {
            fireNow = true;
        }
        else
        {
            listeners.add(listener);
        }

        // the future has already been completed, fire on caller's thread
        if (fireNow)
        {
            listener.handle(this);
        }

    }

    @Override
    public synchronized void removeListener(RiakFutureListener<V> listener)
    {
        if (!listenersFired)
        {
            listeners.remove(listener);
        } // else, we don't care, they've already been fired
    }

    private void fireListeners()
    {

        boolean fireNow = false;
        if (!listenersFired)
        {
            fireNow = true;
            listenersFired = true;
        }

        if (fireNow)
        {
            for (RiakFutureListener<V> listener : listeners)
            {
                listener.handle(this);
            }
        }

    }
}
