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

import java.util.concurrent.ExecutionException;

public class RiakFutures
{

    public interface Func<I, O>
    {
        O apply(I input);
    }

    public interface AsyncFunc<I, O>
    {
        RiakFuture<O> apply(I input);
    }

    public static <U, V> RiakFuture<U> map(RiakFuture<V> future, final Func<V, U> mapping)
    {

        final RiakPromise<U> promise = new RiakPromise<U>();
        future.addListener(new RiakFutureListener<V>()
        {
            @Override
            public void handle(RiakFuture<V> f)
            {
                try
                {
                    if (f.isCancelled())
                    {
                        promise.cancel(false);
                        return;
                    }

                    V value = f.get();
                    U mapped = mapping.apply(value);
                    promise.set(mapped);
                }
                catch (ExecutionException e)
                {
                    promise.setException(e.getCause());
                }
                catch (InterruptedException e)
                {
                    promise.setException(e);
                }
            }
        });
        return promise;
    }

    private static <V> V getUnchecked(RiakFuture<V> future)
    {
        try
        {
            return future.get();
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e.getCause());
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static <U, V> RiakFuture<U> flatMap(RiakFuture<V> future, final AsyncFunc<V, U> mapping)
    {
        RiakFuture<RiakFuture<U>> f = map(future, new Func<V, RiakFuture<U>>()
        {
            @Override
            public RiakFuture<U> apply(V input)
            {
                return mapping.apply(input);
            }
        });

        final RiakPromise<U> promise = new RiakPromise<U>();
        f.addListener(new RiakFutureListener<RiakFuture<U>>()
        {
            @Override
            public void handle(RiakFuture<RiakFuture<U>> outer)
            {
                try
                {
                    RiakFuture<U> inner = outer.get();
                    inner.addListener(new RiakFutureListener<U>()
                    {
                        @Override
                        public void handle(RiakFuture<U> inner)
                        {
                            try
                            {
                                promise.set(inner.get());
                            }
                            catch (Exception e)
                            {
                                promise.setException(e);
                            }
                        }
                    });
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }
        });

        return promise;

    }

}
