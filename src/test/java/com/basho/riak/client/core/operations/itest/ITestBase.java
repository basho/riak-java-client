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
import com.basho.riak.client.query.Location;
import com.basho.riak.client.util.BinaryValue;
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
    protected static boolean legacyRiakSearch;
    protected static BinaryValue bucketName;
    protected static BinaryValue counterBucketType;
    protected static BinaryValue setBucketType;
    protected static BinaryValue mapBucketType;
    
    protected static BinaryValue bucketType;

    @BeforeClass
    public static void setUp() throws UnknownHostException
    {
        testYokozuna = Boolean.parseBoolean(System.getProperty("com.basho.riak.yokozuna"));
        test2i = Boolean.parseBoolean(System.getProperty("com.basho.riak.2i"));
        // You must create a bucket type 'test_type' if you enable this.
        testBucketType = Boolean.parseBoolean(System.getProperty("com.basho.riak.buckettype"));
        testCrdt = Boolean.parseBoolean(System.getProperty("com.basho.riak.crdt"));
        bucketType = BinaryValue.unsafeCreate("test_type".getBytes());
        legacyRiakSearch = Boolean.parseBoolean(System.getProperty("com.basho.riak.riakSearch"));
        bucketName = BinaryValue.unsafeCreate("ITestBase".getBytes());
        RiakNode.Builder builder = new RiakNode.Builder()
                                        .withMinConnections(10);

        /**
         * In order to run the CRDT itests you must first manually
         * create the following bucket types in your riak instance
         * with the corresponding bucket properties.
         *
         * riak-admin bucket-type create maps '{"props":{"allow_mult":true, "datatype": "map"}}'
         * riak-admin bucket type create sets '{"props":{"allow_mult":true, "datatype": "set"}}'
         * riak-admin bucket-type create counters '{"props":{"allow_mult":true, "datatype": "counter"}}'
         */
        counterBucketType = BinaryValue.create("counters");
        setBucketType = BinaryValue.create("sets");
        mapBucketType = BinaryValue.create("maps");

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
    
    public static void resetAndEmptyBucket(BinaryValue name) throws InterruptedException, ExecutionException
    {
        resetAndEmptyBucket(new Location(name));

    }

    protected static void resetAndEmptyBucket(Location location) throws InterruptedException, ExecutionException
    {
       ListKeysOperation.Builder keysOpBuilder = new ListKeysOperation.Builder(location);
        
        ListKeysOperation keysOp = keysOpBuilder.build();
        cluster.execute(keysOp);
        List<BinaryValue> keyList = keysOp.get();
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
        
        for (BinaryValue k : keyList)
        {
            location.setKey(k);
            DeleteOperation.Builder delOpBuilder = new DeleteOperation.Builder(location);
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
            new ResetBucketPropsOperation.Builder(location);
        
        ResetBucketPropsOperation resetOp = resetOpBuilder.build();
        cluster.execute(resetOp);
        resetOp.get();

    }
    
}
