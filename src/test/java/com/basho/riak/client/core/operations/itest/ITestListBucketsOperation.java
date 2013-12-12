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
import com.basho.riak.client.core.operations.ListBucketsOperation;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.ByteArrayWrapper;
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
public class ITestListBucketsOperation extends ITestBase
{
    @Test
    public void testListBuckets() throws InterruptedException, ExecutionException
    {
        // Empty buckets do not show up
        final ByteArrayWrapper key = ByteArrayWrapper.unsafeCreate("my_key".getBytes());
        final String value = "{\"value\":\"value\"}";
        
        RiakObject rObj = new RiakObject().setValue(ByteArrayWrapper.create(value));
        
        StoreOperation storeOp = 
            new StoreOperation.Builder(bucketName)
                .withKey(key)
                .withContent(rObj)
                .build();
        
        cluster.execute(storeOp);
        storeOp.get();
        
        ListBucketsOperation listOp = new ListBucketsOperation.Builder().build();
        cluster.execute(listOp);
        List<ByteArrayWrapper> bucketList = listOp.get();
        assertTrue(bucketList.size() > 0);
        
        boolean found = false;
        for (ByteArrayWrapper baw : bucketList)
        {
            if (baw.toString().equals(bucketName.toString()))
            {
                found = true;
            }
        }
        
        assertTrue(found);
    }
    
    @Test
    public void testLargeBucketList() throws InterruptedException, ExecutionException
    {
        final ByteArrayWrapper key = ByteArrayWrapper.unsafeCreate("my_key".getBytes());
        final String value = "{\"value\":\"value\"}";
        final Semaphore semaphore = new Semaphore(10);
        final CountDownLatch latch = new CountDownLatch(1);
        
        RiakFutureListener<StoreOperation.Response> listener =
            new RiakFutureListener<StoreOperation.Response>() {
                
                private AtomicInteger received = new AtomicInteger();
                
                @Override
                public void handle(RiakFuture<StoreOperation.Response> f)
                {
                    try
                    {
                        f.get();
                    }
                    catch (InterruptedException ex)
                    {
                        throw new RuntimeException(ex);
                    }
                    catch (ExecutionException ex)
                    {
                        throw new RuntimeException(ex);
                    }
                    
                    semaphore.release();
                    received.incrementAndGet();
                    if (received.intValue() == 1000)
                    {
                        latch.countDown();
                    }
                }
            };
        
        for (int i = 0; i < 1000; i++)
        {
            ByteArrayWrapper bName = ByteArrayWrapper.unsafeCreate((bucketName.toString() + i).getBytes());
            RiakObject rObj = new RiakObject().setValue(ByteArrayWrapper.create(value));
            StoreOperation storeOp = 
                new StoreOperation.Builder(bName)
                    .withKey(key)
                    .withContent(rObj)
                    .build();
            storeOp.addListener(listener);
            semaphore.acquire();
            cluster.execute(storeOp);
        }
        
        latch.await();
        
        ListBucketsOperation listOp = new ListBucketsOperation.Builder().build();
        cluster.execute(listOp);
        List<ByteArrayWrapper> bucketList = listOp.get();
        assertTrue(bucketList.size() >= 1000);
        
        for (int i = 0; i < 1000; i++)
        {
            ByteArrayWrapper bName = ByteArrayWrapper.unsafeCreate((bucketName.toString() + i).getBytes());
            resetAndEmptyBucket(bName);
        }
        
    }
}
