/*
 * Copyright 2016 Basho Technologies, Inc.
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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Used when the converted response is available before the core future is complete.
 *
 * @param <T> The core response type.
 * @param <S> The core query info type.
 * @param <T2> The converted response type.
 * @param <S2> The converted query info type.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.1.0
 */
public abstract class ImmediateCoreFutureAdapter<T2,S2,T,S> extends CoreFutureAdapter<T2,S2,T,S>
{
    private final T2 immediateResponse;

    protected ImmediateCoreFutureAdapter(RiakFuture<T, S> coreFuture, T2 immediateResponse)
    {
        super(coreFuture);
        this.immediateResponse = immediateResponse;
    }

    @Override
    public T2 get() throws InterruptedException, ExecutionException
    {
        return immediateResponse;
    }

    @Override
    public T2 get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
        return immediateResponse;
    }

    @Override
    public T2 getNow()
    {
        return immediateResponse;
    }

    @Override
    protected T2 convertResponse(T unused) { return null; }

    @Override
    protected abstract S2 convertQueryInfo(S coreQueryInfo);

    public static abstract class SameQueryInfo<T2,S,T> extends ImmediateCoreFutureAdapter<T2,S,T,S>
    {
        protected SameQueryInfo(RiakFuture<T, S> coreFuture, T2 immediateResponse)
        {
            super(coreFuture, immediateResponse);
        }

        @Override
        protected S convertQueryInfo(S coreQueryInfo)
        {
            return coreQueryInfo;
        }
    }
}
