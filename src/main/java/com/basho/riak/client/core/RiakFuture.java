/*
 * Copyright 2013 Brian Roach <roach at basho dot com>.
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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Currently a placeholder so that we can add/remove functionality from {@code Future<T>}
 * 
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public interface RiakFuture<V> extends Future<V>
{
    @Override
    boolean cancel(boolean mayInterruptIfRunning);
    @Override
    V get() throws InterruptedException, ExecutionException;
    @Override
    V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException;
    @Override
    boolean isCancelled();
    @Override
    boolean isDone();
    RiakResponse getRiakResponse() throws InterruptedException, ExecutionException;
    RiakResponse getRiakResponse(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException, ExecutionException;
    void addListener(RiakFutureListener<V> listener);
    void removeListener(RiakFutureListener<V> listener);
}
