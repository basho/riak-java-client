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
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestListKeysOperation extends ITestBase
{
    private Logger logger = LoggerFactory.getLogger("ITestListKeysOperation");

    @Test
    public void testListNoKeysDefaultType() throws InterruptedException, ExecutionException
    {
        testListNoKeys(Namespace.DEFAULT_BUCKET_TYPE);
    }

    @Test
    public void testListNoKeysTestType() throws InterruptedException, ExecutionException
    {
        assumeTrue(testBucketType);
        testListNoKeys(bucketType.toString());
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

    @Test
    public void testListKeyDefaultType() throws InterruptedException, ExecutionException
    {
        testListKey(Namespace.DEFAULT_BUCKET_TYPE);
    }

    @Test
    public void testListKeyTestType() throws InterruptedException, ExecutionException
    {
        assumeTrue(testBucketType);
        testListKey(bucketType.toString());
    }

    private void testListKey(String bucketType) throws InterruptedException, ExecutionException
    {
        final Namespace ns = new Namespace(bucketType, bucketName.toString() + "_2");
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

        ListKeysOperation klistOp = new ListKeysOperation.Builder(ns).build();
        cluster.execute(klistOp);
        List<BinaryValue> kList = klistOp.get().getKeys();

        assertEquals(kList.size(), 1);
        assertEquals(kList.get(0), key);
        resetAndEmptyBucket(ns);

    }

    @Test
    public void testLargeKeyListDefaultType() throws InterruptedException, ExecutionException
    {
        testLargeKeyList(Namespace.DEFAULT_BUCKET_TYPE);
    }

    @Test
    public void testLargeKeyListTestType() throws InterruptedException, ExecutionException
    {
        assumeTrue(testBucketType);
        testLargeKeyList(bucketType.toString());
    }

    private void testLargeKeyList(String bucketType) throws InterruptedException, ExecutionException
    {
        final String baseKey = "my_key";
        final String value = "{\"value\":\"value\"}";
        final Namespace ns = new Namespace(bucketType, bucketName.toString() + "_3");
        final Semaphore semaphore = new Semaphore(10);
        final CountDownLatch latch = new CountDownLatch(1);
        final int expected = 100;

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
                        semaphore.release();
                        received.incrementAndGet();

                        if (expected == received.intValue())
                        {
                            logger.debug("Executing ListKeys");
                            latch.countDown();
                        }
                    }
                    catch (InterruptedException | ExecutionException ex)
                    {
                        throw new RuntimeException(ex);
                    }

                }
            };

        logger.debug("Inserting data");

        for (int i = 0; i < expected; i++)
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

        ListKeysOperation klistOp = new ListKeysOperation.Builder(ns).build();
        cluster.execute(klistOp);
        List<BinaryValue> kList;
        kList = klistOp.get().getKeys();
        assertEquals(expected, kList.size());

        resetAndEmptyBucket(ns);

    }
}
