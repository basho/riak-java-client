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

import com.basho.riak.client.core.StreamingRiakFuture;
import com.basho.riak.client.core.operations.SecondaryIndexQueryOperation;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.indexes.LongIntIndex;
import com.basho.riak.client.core.query.indexes.StringBinIndex;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @author Alex Moore  <amoore at basho dot com>
 */
public class ITestSecondaryIndexQueryOp extends ITestBase
{
    private static final long bucketSeed = new Random().nextLong();
    private static final String bucketName = "ITestSecondaryIndexQueryOp" + bucketSeed;

    private static final Namespace defaultTypeNamespace = new Namespace(bucketName);
    private static final Namespace typedNamespace = new Namespace(bucketType.toString(), bucketName);
    private static final String incrementingIndexNameString = "test_index";
    private static final String allFivesIndexNameString = "all_fives_index";
    private static final String regexIndexNameString = "regex_index";
    private static final BinaryValue incrementingIndexName = BinaryValue.create(incrementingIndexNameString + "_int");
    private static final BinaryValue allFivesIndexName = BinaryValue.create(allFivesIndexNameString + "_int");
    private static final BinaryValue regexIndexName = BinaryValue.create(regexIndexNameString + "_bin");

    private static final String keyBase = "my_key";
    private static final String value = "value";

    @BeforeClass
    public static void setupSiblingBuckets() throws ExecutionException, InterruptedException
    {
        Assume.assumeTrue(test2i);

        setupIndexTestData(defaultTypeNamespace);

        if(testBucketType)
        {
            setupIndexTestData(typedNamespace);
        }
    }

    @AfterClass
    public static void cleanupBuckets() throws ExecutionException, InterruptedException
    {
        resetAndEmptyBucket(defaultTypeNamespace);
        resetAndEmptyBucket(typedNamespace);
    }

    @Test
    public void testSingleQuerySingleResponseDefaultType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        testSingleQuerySingleResponse(defaultTypeNamespace);
    }

    @Test
    public void testSingleQuerySingleResponseDefaultTypeStreaming() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        testSingleQuerySingleResponseStreaming(defaultTypeNamespace);
    }

    @Test
    public void testSingleQuerySingleResponseTestType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        Assume.assumeTrue(testBucketType);
        testSingleQuerySingleResponse(typedNamespace);
    }

    @Test
    public void testSingleQueryMultipleResponseDefaultType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        testSingleQueryMultipleResponse(defaultTypeNamespace);
    }

    @Test
    public void testSingleQueryMultipleResponseTestType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        Assume.assumeTrue(testBucketType);
        testSingleQueryMultipleResponse(typedNamespace);
    }

    @Test
    public void testRangeQueryDefaultType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        testRangeQuery(defaultTypeNamespace);
    }

    @Test
    public void testRangeQueryTestType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        Assume.assumeTrue(testBucketType);
        testRangeQuery(typedNamespace);
    }

    @Test
    public void testNoSortWithNoPagingDefaultType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        testNoSortWithNoPaging(defaultTypeNamespace);
    }

    @Test
    public void testNoSortWithNoPagingTestType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        Assume.assumeTrue(testBucketType);
        testNoSortWithNoPaging(typedNamespace);
    }

    @Test
    public void testNoSortWithNoPagingTestTypeStreaming() throws InterruptedException, ExecutionException
    {
        testNoSortWithNoPagingStreaming(typedNamespace);
    }

    @Test
    public void testSortWithNoPagingDefaultType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        testSortWithNoPaging(defaultTypeNamespace);
    }

    @Test
    public void testSortWithNoPagingTestType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        Assume.assumeTrue(testBucketType);
        testSortWithNoPaging(typedNamespace);
    }

    @Test
    public void testBucketIndexHack() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);

        SecondaryIndexQueryOperation.Query query =
                new SecondaryIndexQueryOperation.Query.Builder(defaultTypeNamespace, BinaryValue.unsafeCreate("$bucket".getBytes()))
                        .withIndexKey(BinaryValue.create(bucketName))
                        .withReturnKeyAndIndex(true)
                        .build();

        SecondaryIndexQueryOperation queryOp =
                new SecondaryIndexQueryOperation.Builder(query)
                        .build();

        cluster.execute(queryOp);
        SecondaryIndexQueryOperation.Response response = queryOp.get();

        assertEquals(100, response.getEntryList().size());
    }

    @Test
    public void testKeyIndexHack() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);

        SecondaryIndexQueryOperation.Query query =
                new SecondaryIndexQueryOperation.Query.Builder(defaultTypeNamespace, BinaryValue.unsafeCreate("$key".getBytes()))
                        .withRangeStart(BinaryValue.create("my_key10"))
                        .withRangeEnd(BinaryValue.create("my_key19"))
                        .withReturnKeyAndIndex(true)
                        .build();

        SecondaryIndexQueryOperation queryOp =
                new SecondaryIndexQueryOperation.Builder(query)
                        .build();

        cluster.execute(queryOp);
        SecondaryIndexQueryOperation.Response response = queryOp.get();

        assertEquals(10, response.getEntryList().size());
    }

    @Test
    public void testNoSortWithPagingDefaultType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        testNoSortWithPaging(defaultTypeNamespace);
    }

    @Test
    public void testNoSortWithPagingTestType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        Assume.assumeTrue(testBucketType);
        testNoSortWithPaging(typedNamespace);
    }

    @Test
    public void testSortWithPagingDefaultType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        testSortWithPaging(defaultTypeNamespace);
    }

    @Test
    public void testSortWithPagingTestType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        Assume.assumeTrue(testBucketType);
        testSortWithPaging(typedNamespace);
    }

    @Test
    public void testSortWithPagingTestTypeStreaming() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        Assume.assumeTrue(testBucketType);
        testSortWithPagingStreaming(typedNamespace);
    }

    @Test
    public void testRegexTermFilterDefaultType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        testRegexTermFilter(defaultTypeNamespace);
    }

    @Test
    public void testRegexTermFilterTestType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        Assume.assumeTrue(testBucketType);
        testRegexTermFilter(typedNamespace);
    }

    @Test
    public void testExceptionThrownWhenUsingRegexFilterOnIntIndexesDefaultType()
    {
        Assume.assumeTrue(test2i);
        testExceptionThrownWhenUsingRegexFilterOnIntIndexes(defaultTypeNamespace);
    }

    @Test
    public void testExceptionThrownWhenUsingRegexFilterOnIntIndexesTestType()
    {
        Assume.assumeTrue(test2i);
        Assume.assumeTrue(testBucketType);
        testExceptionThrownWhenUsingRegexFilterOnIntIndexes(typedNamespace);
    }

    private void testSingleQuerySingleResponse(Namespace namespace) throws InterruptedException, ExecutionException
    {
        SecondaryIndexQueryOperation.Query query =
            new SecondaryIndexQueryOperation.Query.Builder(namespace, incrementingIndexName)
                .withIndexKey(BinaryValue.unsafeCreate(String.valueOf(5L).getBytes()))
                .build();

        SecondaryIndexQueryOperation queryOp =
            new SecondaryIndexQueryOperation.Builder(query)
                .build();

        cluster.execute(queryOp);
        SecondaryIndexQueryOperation.Response response = queryOp.get();

        assertEquals(1, response.getEntryList().size());
        assertFalse(response.getEntryList().get(0).hasIndexKey());
        assertEquals(keyBase + "5", response.getEntryList().get(0).getObjectKey().toString());

        query =
            new SecondaryIndexQueryOperation.Query.Builder(namespace, incrementingIndexName)
                .withIndexKey(BinaryValue.unsafeCreate(String.valueOf(5L).getBytes()))
                .withReturnKeyAndIndex(true)
                .build();
        queryOp =
            new SecondaryIndexQueryOperation.Builder(query)
                .build();

        cluster.execute(queryOp);
        response = queryOp.get();

        assertEquals(1, response.getEntryList().size());
        assertTrue(response.getEntryList().get(0).hasIndexKey());
        assertEquals(BinaryValue.unsafeCreate("5".getBytes()), response.getEntryList().get(0).getIndexKey());
        assertEquals(keyBase + "5", response.getEntryList().get(0).getObjectKey().toString());
    }

    private void testSingleQuerySingleResponseStreaming(Namespace namespace) throws InterruptedException, ExecutionException
    {
        {
            SecondaryIndexQueryOperation.Query query =
                    new SecondaryIndexQueryOperation.Query.Builder(namespace, incrementingIndexName)
                            .withIndexKey(BinaryValue.unsafeCreate(String.valueOf(5L).getBytes()))
                            .build();


            SecondaryIndexQueryOperation queryOp =
                new SecondaryIndexQueryOperation.Builder(query).streamResults(true)
                    .build();

            final StreamingRiakFuture<SecondaryIndexQueryOperation.Response,
                    SecondaryIndexQueryOperation.Query>
                    future = cluster.execute(queryOp);
            SecondaryIndexQueryOperation.Response syncResponse = queryOp.get();

            List<SecondaryIndexQueryOperation.Response.Entry> streamedResponses = new LinkedList<>();
            for (SecondaryIndexQueryOperation.Response r : future.getResultsQueue())
            {
                streamedResponses.addAll(r.getEntryList());
            }

            assertEquals(0, syncResponse.getEntryList().size());
            assertFalse(syncResponse.hasContinuation());

            assertEquals(1, streamedResponses.size());
            final SecondaryIndexQueryOperation.Response.Entry entry = streamedResponses.get(0);
            assertFalse(entry.hasIndexKey());
            assertEquals(keyBase + "5", entry.getObjectKey().toString());
            assertFalse(syncResponse.hasContinuation());
        }

        {
            SecondaryIndexQueryOperation.Query query2 = new SecondaryIndexQueryOperation.Query.Builder(namespace,
                                                                                                       incrementingIndexName)
                    .withIndexKey(BinaryValue.unsafeCreate(String.valueOf(5L).getBytes()))
                    .withReturnKeyAndIndex(true)
                    .build();
            SecondaryIndexQueryOperation queryOp2 =
                    new SecondaryIndexQueryOperation.Builder(query2).streamResults(true).build();

            final StreamingRiakFuture<SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query>
                    future2 = cluster.execute(queryOp2);

            SecondaryIndexQueryOperation.Response syncResponse2 = queryOp2.get();

            LinkedList<SecondaryIndexQueryOperation.Response.Entry> streamedResponses2 = new LinkedList<>();
            for (SecondaryIndexQueryOperation.Response r : future2.getResultsQueue())
            {
                streamedResponses2.addAll(r.getEntryList());
            }

            assertEquals(0, syncResponse2.getEntryList().size());
            assertFalse(syncResponse2.hasContinuation());

            assertEquals(1, streamedResponses2.size());
            final SecondaryIndexQueryOperation.Response.Entry entry2 = streamedResponses2.get(0);
            assertTrue(entry2.hasIndexKey());
            assertEquals(BinaryValue.unsafeCreate("5".getBytes()), entry2.getIndexKey());
            assertEquals(keyBase + "5", entry2.getObjectKey().toString());
        }
    }

    private void testSingleQueryMultipleResponse(Namespace namespace) throws InterruptedException, ExecutionException
    {
        SecondaryIndexQueryOperation.Query query =
            new SecondaryIndexQueryOperation.Query.Builder(namespace, allFivesIndexName)
                .withIndexKey(BinaryValue.unsafeCreate(String.valueOf(5L).getBytes()))
                .withPaginationSort(true)
                .build();

        SecondaryIndexQueryOperation queryOp =
            new SecondaryIndexQueryOperation.Builder(query)
                .build();

        cluster.execute(queryOp);
        SecondaryIndexQueryOperation.Response response = queryOp.get();

        assertEquals(100, response.getEntryList().size());
        assertFalse(response.getEntryList().get(0).hasIndexKey());
        assertEquals(keyBase + "0", response.getEntryList().get(0).getObjectKey().toString());

        query =
            new SecondaryIndexQueryOperation.Query.Builder(namespace, allFivesIndexName)
                .withIndexKey(BinaryValue.unsafeCreate(String.valueOf(5L).getBytes()))
                .withPaginationSort(true)
                .withReturnKeyAndIndex(true)
                .build();

        queryOp =
            new SecondaryIndexQueryOperation.Builder(query)
                .build();

        cluster.execute(queryOp);
        response = queryOp.get();

        assertEquals(100, response.getEntryList().size());
        assertTrue(response.getEntryList().get(0).hasIndexKey());
        assertEquals(BinaryValue.unsafeCreate("5".getBytes()), response.getEntryList().get(0).getIndexKey());
        assertEquals(keyBase + "0", response.getEntryList().get(0).getObjectKey().toString());

    }

    private void testRangeQuery(Namespace namespace) throws InterruptedException, ExecutionException
    {
        SecondaryIndexQueryOperation.Query query =
            new SecondaryIndexQueryOperation.Query.Builder(namespace, incrementingIndexName)
                .withRangeStart(BinaryValue.unsafeCreate(String.valueOf(5L).getBytes()))
                .withRangeEnd(BinaryValue.unsafeCreate(String.valueOf(20L).getBytes()))
                .withPaginationSort(true)
                .build();

        SecondaryIndexQueryOperation queryOp =
            new SecondaryIndexQueryOperation.Builder(query)
                    .build();

        cluster.execute(queryOp);
        SecondaryIndexQueryOperation.Response response = queryOp.get();

        assertEquals(16, response.getEntryList().size());
        assertFalse(response.getEntryList().get(0).hasIndexKey());
        assertEquals(keyBase + "5", response.getEntryList().get(0).getObjectKey().toString());

        query =
            new SecondaryIndexQueryOperation.Query.Builder(namespace, incrementingIndexName)
                .withRangeStart(BinaryValue.unsafeCreate(String.valueOf(5L).getBytes()))
                    .withRangeEnd(BinaryValue.unsafeCreate(String.valueOf(20L).getBytes()))
                    .withReturnKeyAndIndex(true)
                    .withPaginationSort(true)
                    .build();

        queryOp =
            new SecondaryIndexQueryOperation.Builder(query)
                    .build();

        cluster.execute(queryOp);
        response = queryOp.get();

        assertEquals(16, response.getEntryList().size());
        assertTrue(response.getEntryList().get(0).hasIndexKey());
        assertEquals(BinaryValue.unsafeCreate("5".getBytes()), response.getEntryList().get(0).getIndexKey());
        assertEquals(keyBase + "5", response.getEntryList().get(0).getObjectKey().toString());
    }

    private void testNoSortWithNoPaging(Namespace namespace) throws InterruptedException, ExecutionException
    {
        SecondaryIndexQueryOperation.Query query =
            new SecondaryIndexQueryOperation.Query.Builder(namespace, incrementingIndexName)
                .withRangeStart(BinaryValue.unsafeCreate(String.valueOf(0L).getBytes()))
                .withRangeEnd(BinaryValue.unsafeCreate(String.valueOf(100L).getBytes()))
                .withPaginationSort(false)
                .build();

        SecondaryIndexQueryOperation queryOp =
                new SecondaryIndexQueryOperation.Builder(query)
                        .build();

        cluster.execute(queryOp);
        SecondaryIndexQueryOperation.Response response = queryOp.get();

        assertEquals(100, response.getEntryList().size());
    }

    private void testNoSortWithNoPagingStreaming(Namespace namespace) throws InterruptedException, ExecutionException
    {
        SecondaryIndexQueryOperation.Query query =
                new SecondaryIndexQueryOperation.Query.Builder(namespace, incrementingIndexName)
                        .withRangeStart(BinaryValue.unsafeCreate(String.valueOf(0L).getBytes()))
                        .withRangeEnd(BinaryValue.unsafeCreate(String.valueOf(100L).getBytes()))
                        .withPaginationSort(false)
                        .build();

        SecondaryIndexQueryOperation queryOp =
                new SecondaryIndexQueryOperation.Builder(query)
                        .streamResults(true)
                        .build();

        final StreamingRiakFuture<SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query>
                future = cluster.execute(queryOp);


        final BlockingQueue<SecondaryIndexQueryOperation.Response> resultsQueue = future.getResultsQueue();
        final List<SecondaryIndexQueryOperation.Response.Entry> resultsList = new LinkedList<>();
        int timeouts = 0;

        for (int i = 0; i < 100; i++)
        {
            if(future.isDone())
            {
                for (SecondaryIndexQueryOperation.Response response : resultsQueue)
                {
                    resultsList.addAll(response.getEntryList());
                }
                break;
            }

            final SecondaryIndexQueryOperation.Response chunk = resultsQueue.poll(1, TimeUnit.MILLISECONDS);

            if(chunk != null)
            {
                resultsList.addAll(chunk.getEntryList());
                continue;
            }
            else
            {
                timeouts++;
                if (timeouts == 10)
                {
                    break;
                }
            }
        }

        assertEquals(100, resultsList.size());
        assertEquals(0, future.get().getEntryList().size());
        assertFalse(future.get().hasContinuation());
    }

    private void testSortWithNoPaging(Namespace namespace) throws InterruptedException, ExecutionException
    {
        SecondaryIndexQueryOperation.Query query =
            new SecondaryIndexQueryOperation.Query.Builder(namespace, incrementingIndexName)
                .withRangeStart(BinaryValue.unsafeCreate(String.valueOf(0L).getBytes()))
                .withRangeEnd(BinaryValue.unsafeCreate(String.valueOf(100L).getBytes()))
                .withPaginationSort(true)
                .build();

        SecondaryIndexQueryOperation queryOp =
                new SecondaryIndexQueryOperation.Builder(query)
                        .build();

        cluster.execute(queryOp);
        SecondaryIndexQueryOperation.Response response = queryOp.get();

        assertEquals(100, response.getEntryList().size());

        AssertLongObjectsInOrder(response.getEntryList());
    }



    private void testNoSortWithPaging(Namespace namespace) throws InterruptedException, ExecutionException
    {
        try
        {
            SecondaryIndexQueryOperation.Query query =
                new SecondaryIndexQueryOperation.Query.Builder(namespace, incrementingIndexName)
                    .withRangeStart(BinaryValue.unsafeCreate(String.valueOf(0L).getBytes()))
                    .withRangeEnd(BinaryValue.unsafeCreate(String.valueOf(100L).getBytes()))
                    .withPaginationSort(false)
                    .withMaxResults(20)
                    .build();

            new SecondaryIndexQueryOperation.Builder(query).build();

            fail("Didn't throw IllegalArgumentException");
        }
        catch(IllegalArgumentException ex)
        {
            assertNotNull(ex);
        }
    }

    private void testSortWithPaging(Namespace namespace) throws InterruptedException, ExecutionException
    {
        SecondaryIndexQueryOperation.Query query =
                new SecondaryIndexQueryOperation.Query.Builder(namespace, incrementingIndexName)
                    .withRangeStart(BinaryValue.unsafeCreate(String.valueOf(0L).getBytes()))
                    .withRangeEnd(BinaryValue.unsafeCreate(String.valueOf(100L).getBytes()))
                    .withPaginationSort(true)
                    .withMaxResults(20)
                    .build();

        SecondaryIndexQueryOperation queryOp =
                new SecondaryIndexQueryOperation.Builder(query)
                        .build();

        cluster.execute(queryOp);
        SecondaryIndexQueryOperation.Response response = queryOp.get();

        assertEquals(20, response.getEntryList().size());

        AssertLongObjectsInOrder(response.getEntryList());
    }

    private void testSortWithPagingStreaming(Namespace namespace) throws InterruptedException, ExecutionException
    {
        SecondaryIndexQueryOperation.Query query =
                new SecondaryIndexQueryOperation.Query.Builder(namespace, incrementingIndexName)
                    .withRangeStart(BinaryValue.unsafeCreate(String.valueOf(0L).getBytes()))
                    .withRangeEnd(BinaryValue.unsafeCreate(String.valueOf(100L).getBytes()))
                    .withPaginationSort(true)
                    .withMaxResults(20)
                    .build();

        SecondaryIndexQueryOperation queryOp =
                new SecondaryIndexQueryOperation.Builder(query)
                        .streamResults(true)
                        .build();

        final StreamingRiakFuture<SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query>
                future = cluster.execute(queryOp);

        final BlockingQueue<SecondaryIndexQueryOperation.Response> resultsQueue = future.getResultsQueue();
        final List<SecondaryIndexQueryOperation.Response.Entry> resultsList = new LinkedList<>();
        BinaryValue continuation = null;
        int timeouts = 0;

        while(true)
        {
            if(future.isDone())
            {
                while(resultsQueue.peek() != null)
                {
                    final SecondaryIndexQueryOperation.Response response = resultsQueue.take();
                    resultsList.addAll(response.getEntryList());
                    if(response.hasContinuation() && continuation == null)
                    {
                        continuation = response.getContinuation();
                    }
                }
                break;
            }

            final SecondaryIndexQueryOperation.Response chunk = resultsQueue.poll(1, TimeUnit.MILLISECONDS);

            if(chunk == null)
            {
                if (++timeouts == 10)
                {
                    break;
                }
                else
                {
                    continue;
                }
            }

            resultsList.addAll(chunk.getEntryList());
            if(chunk.hasContinuation() && continuation == null)
            {
                continuation = chunk.getContinuation();
            }
        }

        assertEquals(20, resultsList.size());
        assertEquals(0, future.get().getEntryList().size());
        assertTrue(future.get().hasContinuation());
        assertNotNull(continuation);
        assertEquals(continuation, future.get().getContinuation());

        AssertLongObjectsInOrder(resultsList);
    }

    private void testRegexTermFilter(Namespace namespace) throws InterruptedException, ExecutionException
    {
        SecondaryIndexQueryOperation.Query query =
                new SecondaryIndexQueryOperation.Query.Builder(namespace, regexIndexName)
                    .withRangeStart(BinaryValue.unsafeCreate("foo00".getBytes()))
                    .withRangeEnd(BinaryValue.unsafeCreate("foo19".getBytes()))
                    .withRegexTermFilter(BinaryValue.unsafeCreate("2".getBytes()))
                    .withReturnKeyAndIndex(true)
                    .withPaginationSort(true)
                    .build();

        SecondaryIndexQueryOperation queryOp =
                new SecondaryIndexQueryOperation.Builder(query)
                        .build();

        cluster.execute(queryOp);
        SecondaryIndexQueryOperation.Response response = queryOp.get();

        assertEquals(2, response.getEntryList().size());
        assertEquals(BinaryValue.unsafeCreate("foo02".getBytes()), response.getEntryList().get(0).getIndexKey());
        assertEquals("my_key2", response.getEntryList().get(0).getObjectKey().toString());

        assertEquals(BinaryValue.unsafeCreate("foo12".getBytes()), response.getEntryList().get(1).getIndexKey());
        assertEquals("my_key12", response.getEntryList().get(1).getObjectKey().toString());
    }

    private void testExceptionThrownWhenUsingRegexFilterOnIntIndexes(Namespace namespace)
    {
        try
        {
            SecondaryIndexQueryOperation.Query query =
                new SecondaryIndexQueryOperation.Query.Builder(namespace, incrementingIndexName)
                    .withRangeStart(BinaryValue.unsafeCreate("0".getBytes()))
                    .withRangeEnd(BinaryValue.unsafeCreate("100".getBytes()))
                    .withRegexTermFilter(BinaryValue.unsafeCreate("2".getBytes()))
                    .build();

            new SecondaryIndexQueryOperation.Builder(query)
                    .build();

            fail("Didn't throw IllegalArgumentException");
        }
        catch(IllegalArgumentException ex)
        {
            assertNotNull(ex);
        }
    }

    private static void setupIndexTestData(Namespace ns)
            throws InterruptedException, ExecutionException
    {
        for (long i = 0; i < 100; i++)
        {
            RiakObject obj = new RiakObject().setValue(BinaryValue.create(value));

            obj.getIndexes().getIndex(LongIntIndex.named(incrementingIndexNameString)).add(i);
            obj.getIndexes().getIndex(LongIntIndex.named(allFivesIndexNameString)).add(5L);
            obj.getIndexes().getIndex(StringBinIndex.named(regexIndexNameString)).add("foo" + String.format("%02d", i));

            Location location = new Location(ns, keyBase + i);
            StoreOperation storeOp =
                    new StoreOperation.Builder(location)
                            .withContent(obj)
                            .build();

            cluster.execute(storeOp);
            storeOp.get();
        }
    }

    private void AssertLongObjectsInOrder(List<SecondaryIndexQueryOperation.Response.Entry> entryList)
    {
        final String firstKey = entryList.get(0).getObjectKey().toString();
        Long previousKey = Long.parseLong(firstKey.substring(keyBase.length()));

        for (int j = 1; j < entryList.size(); j++)
        {
            String fullKey = entryList.get(j).getObjectKey().toString();
            Long currentKey = Long.parseLong(fullKey.substring(keyBase.length()));
            assertTrue(previousKey <= currentKey);
            previousKey = currentKey;
        }
    }
}
