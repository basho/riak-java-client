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

package com.basho.riak.client.api;

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import java.util.concurrent.ExecutionException;

/**
 * The base class for all Riak Commands.
 * <p>
 * All the commands the {@link RiakClient} can execute extend this class. 
 * <p>
 * @author Dave Rusek <drusek at basho dot com>
 * @param <T> The response type
 * @param <S> The query info type
 * @since 2.0
 */
public abstract class RiakCommand<T,S>
{
    protected final T execute(RiakCluster cluster) throws ExecutionException, InterruptedException
    {
        RiakFuture<T,S> future = executeAsync(cluster);
        future.await();
        if (future.isSuccess())
        {
            return future.get();
        }
        else
        {
            throw new ExecutionException(future.cause());
        }
        
    }
    protected abstract RiakFuture<T,S> executeAsync(RiakCluster cluster);    
} 

