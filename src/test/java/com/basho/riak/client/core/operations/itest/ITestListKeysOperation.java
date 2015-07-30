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
import com.basho.riak.client.core.RiakResultStreamListener;
import com.basho.riak.client.core.operations.ListKeysOperation;
import com.basho.riak.client.core.operations.StoreOperation;
import static com.basho.riak.client.core.operations.itest.ITestBase.bucketName;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

import org.junit.After;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @author Alex Moore <amoore at basho dot com>
 */
public class ITestListKeysOperation extends ITestBase
{
    private final LinkedBlockingQueue<BinaryValue> streamingResults = new LinkedBlockingQueue<BinaryValue>();

    private final RiakResultStreamListener<ListKeysOperation.Response> streamListener =
            new RiakResultStreamListener<ListKeysOperation.Response>() {
                @Override
                public void handle(ListKeysOperation.Response response) {
                    streamingResults.addAll(response.getKeys());
                }
            };

    @After
    public void clearStreamingResults()
    {
        streamingResults.clear();
    }
    
    @Test
    public void testListNoKeysDefaultType() throws InterruptedException, ExecutionException
    {
        testListNoKeys(Namespace.DEFAULT_BUCKET_TYPE);
        testListNoKeysStreamed(Namespace.DEFAULT_BUCKET_TYPE);
    }
    
    @Test
    public void testListNoKeysTestType() throws InterruptedException, ExecutionException
    {
        assumeTrue(testBucketType);
        testListNoKeys(bucketType.toString());
        testListNoKeysStreamed(bucketType.toString());
    }
    
    private void testListNoKeys(String bucketType) throws InterruptedException, ExecutionException
    {
        Namespace ns = new Namespace(bucketType, bucketName.toString() + "_1");
        ListKeysOperation klistOp = new ListKeysOperation.Builder(ns).build();
        cluster.execute(klistOp);
        List<BinaryValue> kList = klistOp.get().getKeys();
        assertTrue(kList.isEmpty());
        resetAndEmptyBucket(ns);
    }

    private void testListNoKeysStreamed(String bucketType) throws InterruptedException, ExecutionException
    {
        Namespace ns = new Namespace(bucketType, bucketName.toString() + "_1_streaming");

        ListKeysOperation klistOp = new ListKeysOperation.Builder(ns).withResultStreamListener(streamListener).build();
        RiakFuture<ListKeysOperation.Response, Namespace> future = cluster.execute(klistOp);

        future.await();

        assertTrue(streamingResults.isEmpty());
        resetAndEmptyBucket(ns);
    }
    
    @Test
    public void testListKeyDefaultType() throws InterruptedException, ExecutionException
    {
        testListKey(Namespace.DEFAULT_BUCKET_TYPE);
        testListKeysStreamed(Namespace.DEFAULT_BUCKET_TYPE);
    }
    
    @Test
    public void testListKeyTestType() throws InterruptedException, ExecutionException
    {
        assumeTrue(testBucketType);
        testListKey(bucketType.toString());
        testListKeysStreamed(bucketType.toString());

    }
    
    private void testListKey(String bucketType) throws InterruptedException, ExecutionException
    {
        final Namespace ns = new Namespace(bucketType, bucketName.toString() + "_2");
        final BinaryValue key = generateObject(ns);
        
        ListKeysOperation klistOp = new ListKeysOperation.Builder(ns).build();
        cluster.execute(klistOp);
        List<BinaryValue> kList = klistOp.get().getKeys();
        
        assertEquals(kList.size(), 1);
        assertEquals(kList.get(0), key);
        resetAndEmptyBucket(ns);
    }

    private void testListKeysStreamed(String bucketType) throws InterruptedException, ExecutionException
    {
        final Namespace ns = new Namespace(bucketType, bucketName.toString() + "_2_streaming");
        final BinaryValue key = generateObject(ns);

        final LinkedBlockingQueue<BinaryValue> results = new LinkedBlockingQueue<BinaryValue>();

        ListKeysOperation klistOp = new ListKeysOperation.Builder(ns).withResultStreamListener(streamListener).build();
        RiakFuture<ListKeysOperation.Response, Namespace> future = cluster.execute(klistOp);

        future.await();

        assertEquals(streamingResults.size(), 1);
        assertEquals(streamingResults.take(), key);
        resetAndEmptyBucket(ns);
    }

    private BinaryValue generateObject(Namespace ns) throws InterruptedException, ExecutionException {
        final BinaryValue key = BinaryValue.unsafeCreate("my_key".getBytes());
        final String value = "{\"value\":\"value\"}";

        RiakObject rObj = new RiakObject().setValue(BinaryValue.create(value));
        Location location = new Location(ns, key);
        StoreOperation storeOp =
                new StoreOperation.Builder(location)
                        .withContent(rObj)
                        .build();

        cluster.execute(storeOp);
        storeOp.get();
        return key;
    }

    @Test
    public void testLargeKeyListDefaultType() throws InterruptedException, ExecutionException
    {
        testLargeKeyList(Namespace.DEFAULT_BUCKET_TYPE);
        testLargeKeyListStreamed(Namespace.DEFAULT_BUCKET_TYPE);
    }
    
    @Test
    public void testLargeKeyListTestType() throws InterruptedException, ExecutionException
    {
        assumeTrue(testBucketType);
        testLargeKeyList(bucketType.toString());
        testLargeKeyListStreamed(bucketType.toString());
    }
    
    private void testLargeKeyList(String bucketType) throws InterruptedException, ExecutionException
    {
        final Namespace ns = new Namespace(bucketType, bucketName.toString() + "_3");

        generateManyObjects(ns);

        ListKeysOperation klistOp = new ListKeysOperation.Builder(ns).build();
        cluster.execute(klistOp);
        List<BinaryValue> kList;
        kList = klistOp.get().getKeys();
        assertEquals(kList.size(), 1000);

        ITestBase.resetAndEmptyBucket(ns);
    }

    private void testLargeKeyListStreamed(String bucketType) throws InterruptedException, ExecutionException
    {
        final Namespace ns = new Namespace(bucketType, bucketName.toString() + "_3");

        generateManyObjects(ns);

        ListKeysOperation klistOp = new ListKeysOperation.Builder(ns).withResultStreamListener(streamListener).build();
        RiakFuture<ListKeysOperation.Response, Namespace> future = cluster.execute(klistOp);

        future.await();

        assertEquals(streamingResults.size(), 1000);

        ITestBase.resetAndEmptyBucket(ns);
    }

    private void generateManyObjects(Namespace ns) throws InterruptedException {
        final String baseKey = "my_key";
        final String value = "{\"value\":\"value\"}";
        final Semaphore semaphore = new Semaphore(10);
        final CountDownLatch latch = new CountDownLatch(1);

        RiakFutureListener<StoreOperation.Response, Location> listener =
            new RiakFutureListener<StoreOperation.Response, Location>() {

            private final int expected = 1000;
            private final AtomicInteger received = new AtomicInteger();

            @Override
            public void handle(RiakFuture<StoreOperation.Response, Location> f)
            {
                try
                {
                    f.get();
                    semaphore.release();
                    received.incrementAndGet();

                    if (expected == received.intValue())
                    {
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
            Location location = new Location(ns, key);
            StoreOperation storeOp =
                new StoreOperation.Builder(location)
                .withContent(rObj)
                .build();

            storeOp.addListener(listener);
            cluster.execute(storeOp);
        }

        latch.await(2, TimeUnit.MINUTES);
    }
}
