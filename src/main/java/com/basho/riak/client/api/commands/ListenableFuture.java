/*
 * Copyright 2014 Brian Roach <roach at basho dot com>.
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

package com.basho.riak.client.api.commands;

import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakFutureListener;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The base class for RiakFutures returned to users.
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public abstract class ListenableFuture<T,S> implements RiakFuture<T,S>
{
    private final Set<RiakFutureListener<T,S>> listeners = new HashSet<>();
    private final ReentrantLock listenersLock = new ReentrantLock();

    @Override
    public void addListener(RiakFutureListener<T, S> listener)
    {
        listenersLock.lock();
        try
        {
            if (isDone())
            {
                listener.handle(this);
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
    }

    @Override
    public void removeListener(RiakFutureListener<T, S> listener)
    {
        listenersLock.lock();
        try
        {
            listeners.remove(listener);
        }
        finally
        {
            listenersLock.unlock();
        }
    }

    protected void notifyListeners()
    {
        listenersLock.lock();
        try
        {
            for (RiakFutureListener<T,S> listener : listeners)
            {
                listener.handle(this);
            }
        }
        finally
        {
            listenersLock.unlock();
        }
    }
}
