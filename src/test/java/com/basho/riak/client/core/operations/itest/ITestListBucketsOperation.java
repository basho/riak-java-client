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
import com.basho.riak.client.core.StreamingRiakFuture;
import com.basho.riak.client.core.operations.ListBucketsOperation;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.core.operations.StreamingListBucketsOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
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

    @Test
    public void testLargeBucketListTestTypeStreaming() throws InterruptedException, ExecutionException
    {
        assumeTrue(testBucketType);
        testLargeBucketListStreaming(bucketType.toString());
    }

    private void testLargeBucketList(String bucketType) throws InterruptedException, ExecutionException
    {
        final int bucketCount = 10;
        final List<BinaryValue> bucketNames = storeObjects(bucketType, bucketCount);

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

    private void testLargeBucketListStreaming(String bucketType) throws InterruptedException, ExecutionException
    {
        final int bucketCount = 10;
        final List<BinaryValue> expectedBucketNames = storeObjects(bucketType, bucketCount);

        StreamingListBucketsOperation listOp = new StreamingListBucketsOperation.Builder()
                .withBucketType(BinaryValue.createFromUtf8(bucketType))
                .build();

        final StreamingRiakFuture<BinaryValue, BinaryValue> execute = cluster.execute(listOp);
        final BlockingQueue<BinaryValue> resultsQueue = execute.getResultsQueue();

        List<BinaryValue> actualBucketNames = new LinkedList<>();
        int timeouts = 0;

        for (int i = 0; i < bucketCount; i++)
        {
            final BinaryValue bucket = resultsQueue.poll(1, TimeUnit.SECONDS);

            if(bucket != null)
            {
                actualBucketNames.add(bucket);
                continue;
            }

            timeouts++;
            if(timeouts == 10)
            {
                break;
            }
        }

        assertEquals(bucketCount, actualBucketNames.size());

        for (BinaryValue name : expectedBucketNames)
        {
            assertTrue(actualBucketNames.contains(name));
        }

        for (BinaryValue name : expectedBucketNames)
        {
            Namespace ns = new Namespace(bucketType, name.toString());
            resetAndEmptyBucket(ns);
        }
    }

    private List<BinaryValue> storeObjects(String bucketType, int bucketCount) throws InterruptedException
    {
        final List<BinaryValue> bucketNames = new ArrayList<>();
        final BinaryValue key = BinaryValue.unsafeCreate("my_key".getBytes());
        final String value = "{\"value\":\"value\"}";

        for (int i = 0; i < bucketCount; i++)
        {
            String testBucketName = bucketName.toString() + i;
            bucketNames.add(BinaryValue.create(testBucketName));

            Namespace ns = new Namespace(bucketType, testBucketName);
            RiakObject rObj = new RiakObject().setValue(BinaryValue.create(value));
            Location location = new Location(ns, key);
            StoreOperation storeOp = new StoreOperation.Builder(location)
                                                       .withContent(rObj)
                                                       .build();

            final RiakFuture<StoreOperation.Response, Location> execute = cluster.execute(storeOp);
            execute.await();
            assertTrue(execute.isSuccess());
        }

        return bucketNames;
    }

}
