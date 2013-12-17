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

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakFutureListener;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.DeleteOperation;
import com.basho.riak.client.core.operations.ListKeysOperation;
import com.basho.riak.client.core.operations.ResetBucketPropsOperation;
import com.basho.riak.client.util.ByteArrayWrapper;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public abstract class ITestBase
{
    protected static RiakCluster cluster;
    protected static boolean testYokozuna;
    protected static boolean test2i;
    protected static boolean testBucketType;
    protected static boolean testCrdt;
    protected static boolean riakSearch;
    protected static ByteArrayWrapper bucketName;
    protected static ByteArrayWrapper counterBucketType;
    protected static ByteArrayWrapper setBucketType;
    protected static ByteArrayWrapper mapBucketType;
    
    protected static ByteArrayWrapper bucketType;

    @BeforeClass
    public static void setUp() throws UnknownHostException
    {
        testYokozuna = Boolean.parseBoolean(System.getProperty("com.basho.riak.yokozuna"));
        test2i = Boolean.parseBoolean(System.getProperty("com.basho.riak.2i"));
        // You must create a bucket type 'test_type' if you enable this.
        testBucketType = Boolean.parseBoolean(System.getProperty("com.basho.riak.buckettype"));
        testCrdt = Boolean.parseBoolean(System.getProperty("com.basho.riak.crdt"));
        bucketType = ByteArrayWrapper.unsafeCreate("test_type".getBytes());
        riakSearch = Boolean.parseBoolean(System.getProperty("com.basho.riak.riakSearch"));
        bucketName = ByteArrayWrapper.unsafeCreate("ITestBase".getBytes());
        RiakNode.Builder builder = new RiakNode.Builder()
                                        .withMinConnections(10);

        /**
         * In order to run the CRDT itests you must first manually
         * create the following bucket types in your riak instance
         * with the corresponding bucket properties.
         *
         * maps: allow_mult = true, datatype = asMap
         * sets: allow_mult = true, datatype = asSet
         * counters: allow_mult = true, datatype = counter
         */
        counterBucketType = ByteArrayWrapper.create("counters");
        setBucketType = ByteArrayWrapper.create("sets");
        mapBucketType = ByteArrayWrapper.create("maps");

        cluster = new RiakCluster.Builder(builder.build()).build();
        cluster.start();
    }
    
    @Before
    public void beforeTest() throws InterruptedException, ExecutionException
    {
        resetAndEmptyBucket(bucketName);
    }
    
    @AfterClass
    public static void tearDown() throws InterruptedException, ExecutionException
    {
        resetAndEmptyBucket(bucketName);
        cluster.shutdown().get();
    }
    
    public static void resetAndEmptyBucket(ByteArrayWrapper name) throws InterruptedException, ExecutionException
    {
        resetAndEmptyBucket(name, null);

    }

    protected static void resetAndEmptyBucket(ByteArrayWrapper name, ByteArrayWrapper type) throws InterruptedException, ExecutionException
    {
        ListKeysOperation.Builder keysOpBuilder = new ListKeysOperation.Builder(name);
        if (type != null)
        {
            keysOpBuilder.withBucketType(type);
        }

        ListKeysOperation keysOp = keysOpBuilder.build();
        cluster.execute(keysOp);
        List<ByteArrayWrapper> keyList = keysOp.get();
        final int totalKeys = keyList.size();
        final Semaphore semaphore = new Semaphore(10);
        final CountDownLatch latch = new CountDownLatch(1);
        
        RiakFutureListener<Boolean> listener = new RiakFutureListener<Boolean>() {

            private final AtomicInteger received = new AtomicInteger();
            
            @Override
            public void handle(RiakFuture<Boolean> f)
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
                if (received.intValue() == totalKeys)
                {
                    latch.countDown();
                }
            }
            
        };
        
        for (ByteArrayWrapper k : keyList)
        {
            DeleteOperation.Builder delOpBuilder = new DeleteOperation.Builder(name, k);
            if (type != null)
            {
                delOpBuilder.withBucketType(type);
            }
            DeleteOperation delOp = delOpBuilder.build();
            delOp.addListener(listener);
            semaphore.acquire();
            cluster.execute(delOp);
        }

        if (!keyList.isEmpty())
        {
            latch.await();
        }
        
        ResetBucketPropsOperation.Builder resetOpBuilder = 
            new ResetBucketPropsOperation.Builder(name);
        if (type != null)
        {
            resetOpBuilder.withBucketType(type);
        }

        ResetBucketPropsOperation resetOp = resetOpBuilder.build();
        cluster.execute(resetOp);
        resetOp.get();

    }
    
}
