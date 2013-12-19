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

import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

@Ignore
public class AbstractRiakFutureTest
{

    @Test
    public void testSet()
    {

        RiakPromise<Integer> p = new RiakPromise<Integer>();
        p.addListener(new RiakFutureListener<Integer>()
        {
            @Override
            public void handle(RiakFuture<Integer> f)
            {
                try
                {
                    System.out.println(f.get());
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
                catch (ExecutionException e)
                {
                    e.printStackTrace();
                }
            }
        });

        p.setException(new Exception("hi"));
    }

    @Test
    public void testMap() throws ExecutionException, InterruptedException
    {

        RiakPromise<Integer> input = new RiakPromise<Integer>();

        RiakFuture<Integer> output = RiakFutures.map(input, new RiakFutures.Func<Integer, Integer>()
        {
            @Override
            public Integer apply(Integer input)
            {
                return input + 10;
            }
        });

        input.set(1);
        System.out.println(output.get());

    }

    @Test
    public void testFlatMap() throws ExecutionException, InterruptedException
    {
        RiakPromise<Integer> input = new RiakPromise<Integer>();

        RiakFuture<Integer> output = RiakFutures.flatMap(input, new RiakFutures.AsyncFunc<Integer, Integer>()
        {
            @Override
            public RiakFuture<Integer> apply(Integer input)
            {
                RiakPromise<Integer> out = new RiakPromise<Integer>();
                out.set(input + 10);
                return out;
            }
        });

        input.set(1);
        System.out.println(output.get());
    }


}
