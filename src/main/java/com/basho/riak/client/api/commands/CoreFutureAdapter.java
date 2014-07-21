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
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 * @param <T> The core response type.
 * @param <S> The core query info type.
 * @param <T2> The converted response type.
 * @param <S2> The converted query info type.
 */
public abstract class CoreFutureAdapter<T2,S2,T,S> extends ListenableFuture<T2,S2> implements RiakFutureListener<T,S>
{
    private final RiakFuture<T,S> coreFuture;
    
    public CoreFutureAdapter(RiakFuture<T,S> coreFuture)
    {
        this.coreFuture = coreFuture;
    }
    
    @Override
    public boolean cancel(boolean mayInterruptIfRunning)
    {
        return coreFuture.cancel(mayInterruptIfRunning);
    }

    @Override
    public T2 get() throws InterruptedException
    {
        T response = coreFuture.get();
        if (response != null)
        {
            return convertResponse(coreFuture.get());
        }
        else 
        {
            return null;
        }
    }

    @Override
    public T2 get(long timeout, TimeUnit unit) throws InterruptedException
    {
        T response = coreFuture.get(timeout, unit);
        if (response != null)
        {
            return convertResponse(coreFuture.get(timeout, unit));
        }
        else
        {
            return null;
        }
    }

    @Override
    public boolean isCancelled()
    {
        return coreFuture.isCancelled();
    }

    @Override
    public boolean isDone()
    {
        return coreFuture.isDone();
    }

    @Override
    public void await() throws InterruptedException
    {
        coreFuture.await();
    }

    @Override
    public void await(long timeout, TimeUnit unit) throws InterruptedException
    {
        coreFuture.await(timeout, unit);
    }

    @Override
    public boolean isSuccess()
    {
        return coreFuture.isSuccess();
    }

    @Override
    public Throwable cause()
    {
        return coreFuture.cause();
    }

    @Override
    public S2 getQueryInfo()
    {
        return convertQueryInfo(coreFuture.getQueryInfo());
    }
    
    @Override
    public void handle(RiakFuture<T,S> f)
    {
        notifyListeners();
    }
    
    protected abstract T2 convertResponse(T coreResponse);
    protected abstract S2 convertQueryInfo(S coreQueryInfo);
}
