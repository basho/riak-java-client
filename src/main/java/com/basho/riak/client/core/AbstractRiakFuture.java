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
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractRiakFuture<V> implements RiakFuture<V>
{

    private final SettableCallable<V> settable = new SettableCallable<V>();
    private final SettableTask<V> task = new SettableTask<V>(settable);

    private final ReentrantLock listenersLock = new ReentrantLock();
    private final HashSet<RiakFutureListener<V>> listeners =
        new HashSet<RiakFutureListener<V>>();
    private volatile boolean listenersFired = false;

    @Override
    public boolean isCancelled()
    {
        return task.isCancelled();
    }

    @Override
    public boolean isDone()
    {
        return task.isDone();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning)
    {
        return task.cancel(mayInterruptIfRunning);
    }

    @Override
    public V get() throws InterruptedException, ExecutionException
    {
        return task.get();
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
        return task.get(timeout, unit);
    }

    private void doDone()
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
            for (RiakFutureListener<V> listener : listeners)
            {
                listener.handle(this);
            }
        }
    }

    @Override
    public void addListener(RiakFutureListener<V> listener)
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
    public void removeListener(RiakFutureListener<V> listener)
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

    protected boolean set(V v)
    {
        boolean set;
        if ((set = settable.set(v)))
        {
            task.run();
        }
        return set;
    }

    protected boolean setException(Throwable t)
    {
        boolean set;
        if ((set = settable.setException(t)))
        {
            task.run();
        }
        return set;
    }

    private class SettableTask<V> extends FutureTask<V>
    {
        private SettableTask(Callable<V> callable)
        {
            super(callable);
        }

        @Override
        protected void done()
        {
            doDone();
        }

    }

    private static class SettableCallable<V> implements Callable<V>
    {

        private volatile Throwable t;
        private volatile V v;

        @Override
        public V call() throws Exception
        {
            if (v != null)
            {
                return v;
            }
            throw new ExecutionException(t);
        }

        private boolean isSet()
        {
            return (v != null) || (t != null);
        }

        public synchronized boolean setException(Throwable t)
        {
            if (!isSet())
            {
                this.t = t;
                return true;
            }
            return false;
        }

        public synchronized boolean set(V v)
        {
            if (!isSet())
            {
                this.v = v;
                return true;
            }
            return false;
        }
    }


}
