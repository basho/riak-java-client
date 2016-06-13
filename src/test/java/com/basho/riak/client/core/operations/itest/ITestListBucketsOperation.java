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
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestListBucketsOperation extends ITestAutoCleanupBase
{
    @Test
    public void testListBucketsDefaultType() throws InterruptedException, ExecutionException
    {
        testListBuckets(Namespace.DEFAULT_BUCKET_TYPE);
    }
    
    @Test
    public void testListBucketsTestType() throws InterruptedException, ExecutionException
    {
        assumeTrue(testBucketType);
        testListBuckets(bucketType.toString());
    }
    
    private void testListBuckets(String bucketType) throws InterruptedException, ExecutionException
    {
        // Empty buckets do not show up
        final BinaryValue key = BinaryValue.unsafeCreate("my_key".getBytes());
        final String value = "{\"value\":\"value\"}";
        
        RiakObject rObj = new RiakObject().setValue(BinaryValue.create(value));
        Location location = new Location(new Namespace(bucketType, bucketName.toString()), key);
        StoreOperation storeOp = 
            new StoreOperation.Builder(location)
                .withContent(rObj)
                .build();
        
        cluster.execute(storeOp);
        storeOp.get();
        
        ListBucketsOperation listOp = new ListBucketsOperation.Builder()
                                        .withBucketType(BinaryValue.createFromUtf8(bucketType))
                                        .build();
        cluster.execute(listOp);
        List<BinaryValue> bucketList = listOp.get().getBuckets();
        assertTrue(bucketList.size() > 0);
        
        boolean found = false;
        for (BinaryValue baw : bucketList)
        {
            if (baw.toString().equals(bucketName.toString()))
            {
                found = true;
            }
        }
        
        assertTrue(found);
    }
    
    @Test
    public void testLargeBucketListDefaultType() throws InterruptedException, ExecutionException
    {
        testLargeBucketList(Namespace.DEFAULT_BUCKET_TYPE);
    }
    
    @Test
    public void testLargeBucketListTestType() throws InterruptedException, ExecutionException
    {
        assumeTrue(testBucketType);
        testLargeBucketList(bucketType.toString());
    }
    
    private void testLargeBucketList(String bucketType) throws InterruptedException, ExecutionException
    {
        final BinaryValue key = BinaryValue.unsafeCreate("my_key".getBytes());
        final String value = "{\"value\":\"value\"}";
        final Semaphore semaphore = new Semaphore(10);
        final CountDownLatch latch = new CountDownLatch(1);
        final int bucketCount = 10;
        final List<BinaryValue> bucketNames = new ArrayList<>();

        RiakFutureListener<StoreOperation.Response, Location> listener =
            new RiakFutureListener<StoreOperation.Response, Location>()
            {
                
                private final AtomicInteger received = new AtomicInteger();
                
                @Override
                public void handle(RiakFuture<StoreOperation.Response, Location> f)
                {
                    try
                    {
                        f.get();
                    }
                    catch (InterruptedException | ExecutionException ex)
                    {
                        throw new RuntimeException(ex);
                    }

                    semaphore.release();
                    received.incrementAndGet();
                    if (received.intValue() == bucketCount)
                    {
                        latch.countDown();
                    }
                }
            };
        
        for (int i = 0; i < bucketCount; i++)
        {
            String testBucketName = bucketName.toString() + i;
            bucketNames.add(BinaryValue.create(testBucketName));

            Namespace ns = new Namespace(bucketType, testBucketName);
            RiakObject rObj = new RiakObject().setValue(BinaryValue.create(value));
            Location location = new Location(ns, key);
            StoreOperation storeOp = 
                new StoreOperation.Builder(location)
                    .withContent(rObj)
                    .build();
            storeOp.addListener(listener);
            semaphore.acquire();
            cluster.execute(storeOp);
        }
        
        latch.await(2, TimeUnit.MINUTES);
        
        ListBucketsOperation listOp = new ListBucketsOperation.Builder()
                                        .withBucketType(BinaryValue.createFromUtf8(bucketType))
                                        .build();
        cluster.execute(listOp);
        List<BinaryValue> bucketList = listOp.get().getBuckets();

        for (BinaryValue name : bucketNames)
        {
            assertTrue(bucketList.contains(name));
        }

        for (BinaryValue name : bucketNames)
        {
            Namespace ns = new Namespace(bucketType, name.toString());
            resetAndEmptyBucket(ns);
        }
        
    }
}
