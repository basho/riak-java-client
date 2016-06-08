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
    private String defaultBucketType = Namespace.DEFAULT_BUCKET_TYPE;
    private String namedBucketType = ITestBase.bucketType.toStringUtf8();

    @Test
    public void testListBucketsDefaultType() throws InterruptedException, ExecutionException
    {
        testBucketList(defaultBucketType, 1);
    }

    @Test
    public void testListBucketsTestType() throws InterruptedException, ExecutionException
    {
        assumeTrue(testBucketType);
        testBucketList(namedBucketType, 1);
    }

    @Test
    public void testListBucketsDefaultTypeStreaming() throws ExecutionException, InterruptedException
    {
        testBucketListStreaming(namedBucketType, 1);
    }

    @Test
    public void testLargeBucketListDefaultType() throws InterruptedException, ExecutionException
    {
        testBucketList(defaultBucketType, 10);
    }

    @Test
    public void testLargeBucketListTestType() throws InterruptedException, ExecutionException
    {
        assumeTrue(testBucketType);
        testBucketList(namedBucketType, 10);
    }

    @Test
    public void testLargeBucketListDefaultTypeStreaming() throws InterruptedException, ExecutionException
    {
        assumeTrue(testBucketType);
        testBucketListStreaming(defaultBucketType, 10);
    }

    @Test
    public void testLargeBucketListTestTypeStreaming() throws InterruptedException, ExecutionException
    {
        assumeTrue(testBucketType);
        testBucketListStreaming(namedBucketType, 10);
    }

    private void testBucketList(String bucketType, int bucketCount) throws InterruptedException, ExecutionException
    {
        final List<BinaryValue> expectedBuckets = storeObjects(bucketType, bucketCount);

        List<BinaryValue> actualBucketNames = getAllBucketListResults(bucketType);

        assertContainsAll(expectedBuckets, actualBucketNames);

        resetAndEmptyBuckets(bucketType, expectedBuckets);
    }

    private void testBucketListStreaming(String bucketType, int bucketCount) throws InterruptedException, ExecutionException
    {
        final List<BinaryValue> expectedBuckets = storeObjects(bucketType, bucketCount);
        final StreamingListBucketsOperation listOp = new StreamingListBucketsOperation.Builder()
                .withBucketType(BinaryValue.createFromUtf8(bucketType))
                .build();

        final StreamingRiakFuture<BinaryValue, BinaryValue> execute = cluster.execute(listOp);
        final BlockingQueue<BinaryValue> resultsQueue = execute.getResultsQueue();

        List<BinaryValue> actualBuckets = new LinkedList<>();
        int timeouts = 0;

        while(!execute.isDone())
        {
            final BinaryValue bucket = resultsQueue.poll(50, TimeUnit.MILLISECONDS);

            if(bucket != null)
            {
                actualBuckets.add(bucket);
                continue;
            }

            timeouts++;
            if(timeouts == 10)
            {
                break;
            }
        }

        // Grab any last buckets that came in on the last message
        resultsQueue.drainTo(actualBuckets);

        assertContainsAll(expectedBuckets, actualBuckets);

        resetAndEmptyBuckets(bucketType, expectedBuckets);
    }

    private void assertContainsAll(List<BinaryValue> expectedSet, List<BinaryValue> actualSet)
    {
        for (BinaryValue name : expectedSet)
        {
            assertTrue(actualSet.contains(name));
        }
    }

    private void resetAndEmptyBuckets(String bucketType, List<BinaryValue> expectedBucketNames)
            throws InterruptedException, ExecutionException
    {
        for (BinaryValue name : expectedBucketNames)
        {
            Namespace ns = new Namespace(bucketType, name.toString());
            resetAndEmptyBucket(ns);
        }
    }

    private List<BinaryValue> getAllBucketListResults(String bucketType) throws InterruptedException, ExecutionException
    {
        final ListBucketsOperation listOp = new ListBucketsOperation.Builder()
                .withBucketType(BinaryValue.createFromUtf8(bucketType))
                .build();
        cluster.execute(listOp);
        return listOp.get().getBuckets();
    }

    private List<BinaryValue> storeObjects(String bucketType, int bucketCount) throws InterruptedException
    {
        final List<BinaryValue> bucketNames = new ArrayList<>();
        final BinaryValue key = BinaryValue.unsafeCreate("my_key".getBytes());
        final String value = "{\"value\":\"value\"}";
        final RiakObject rObj = new RiakObject().setValue(BinaryValue.create(value));

        for (int i = 0; i < bucketCount; i++)
        {
            final String testBucketName = testName.getMethodName() + "_" + i;
            bucketNames.add(BinaryValue.create(testBucketName));

            final Location location = new Location(new Namespace(bucketType, testBucketName), key);
            final StoreOperation storeOp = new StoreOperation.Builder(location)
                                                       .withContent(rObj)
                                                       .build();

            final RiakFuture<StoreOperation.Response, Location> execute = cluster.execute(storeOp);
            execute.await();
            assertTrue(execute.isSuccess());
        }

        return bucketNames;
    }

}
