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
package com.basho.riak.client.core.operations.itest;

import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakFutureListener;
import com.basho.riak.client.core.operations.ListKeysOperation;
import com.basho.riak.client.core.operations.StoreOperation;
import static com.basho.riak.client.core.operations.itest.ITestBase.bucketName;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.BinaryValue;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestListKeysOperation extends ITestBase
{
    
    
    @Test
    public void testListNoKeys() throws InterruptedException, ExecutionException
    {
        final BinaryValue bName = BinaryValue.unsafeCreate((bucketName.toString() + "_1").getBytes());
        Location location = new Location(bName);
        ListKeysOperation klistOp = new ListKeysOperation.Builder(location).build();
        cluster.execute(klistOp);
        List<BinaryValue> kList = klistOp.get();
        assertTrue(kList.isEmpty());
        resetAndEmptyBucket(bName);

    }
    
    @Test
    public void testListKey() throws InterruptedException, ExecutionException
    {
        final BinaryValue bName = BinaryValue.unsafeCreate((bucketName.toString() + "_2").getBytes());
        final BinaryValue key = BinaryValue.unsafeCreate("my_key".getBytes());
        final String value = "{\"value\":\"value\"}";
        
        RiakObject rObj = new RiakObject().setValue(BinaryValue.create(value));
        Location location = new Location(bName).setKey(key);
        StoreOperation storeOp = 
            new StoreOperation.Builder(location)
                .withContent(rObj)
                .build();
        
        cluster.execute(storeOp);
        storeOp.get();
        
        location = new Location(bName);
        ListKeysOperation klistOp = new ListKeysOperation.Builder(location).build();
        cluster.execute(klistOp);
        List<BinaryValue> kList = klistOp.get();
        
        assertEquals(kList.size(), 1);
        assertEquals(kList.get(0), key);
        resetAndEmptyBucket(bName);

    }
    
    @Test
    public void testLargeKeyList() throws InterruptedException, ExecutionException
    {
        final String baseKey = "my_key";
        final String value = "{\"value\":\"value\"}";
        final BinaryValue bName = BinaryValue.unsafeCreate((bucketName.toString() + "_3").getBytes());
        final Semaphore semaphore = new Semaphore(10);
        final CountDownLatch latch = new CountDownLatch(1);
        
        RiakFutureListener<StoreOperation.Response> listener =
            new RiakFutureListener<StoreOperation.Response>() {
            
            private final int expected = 1000;
            private final AtomicInteger received = new AtomicInteger();
            
            @Override
            public void handle(RiakFuture<StoreOperation.Response> f)
            {
                try 
                {
                    f.get();
                    semaphore.release();
                    received.incrementAndGet();

                    if (expected == received.intValue())
                    {
                        Location location = new Location(bName);
                        ListKeysOperation klistOp = new ListKeysOperation.Builder(location).build();
                        cluster.execute(klistOp);
                        List<BinaryValue> kList;
                        kList = klistOp.get();
                        assertEquals(kList.size(), 1000);
                        latch.countDown();
                    }
                }
                catch (InterruptedException ex)
                {
                    throw new RuntimeException(ex);
                }
                catch (ExecutionException ex)
                {
                    throw new RuntimeException(ex);
                }
            }
        };
        
        for (int i = 0; i < 1000; i++)
        {
            semaphore.acquire();
            BinaryValue key = BinaryValue.unsafeCreate((baseKey + i).getBytes());
            RiakObject rObj = new RiakObject().setValue(BinaryValue.create(value));
            Location location = new Location(bName).setKey(key);
            StoreOperation storeOp = 
                new StoreOperation.Builder(location)
                .withContent(rObj)
                .build();
        
            storeOp.addListener(listener);
            cluster.execute(storeOp);
        }
        
        latch.await();
        ITestBase.resetAndEmptyBucket(bName);
        
    }
}
