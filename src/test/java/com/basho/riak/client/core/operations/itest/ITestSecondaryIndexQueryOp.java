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

import com.basho.riak.client.core.operations.SecondaryIndexQueryOperation;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.indexes.LongIntIndex;
import com.basho.riak.client.core.query.indexes.StringBinIndex;
import com.basho.riak.client.core.util.BinaryValue;

import java.util.concurrent.ExecutionException;
import org.junit.Assume;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @author Alex Moore  <amoore at basho dot com>
 */
public class ITestSecondaryIndexQueryOp extends ITestBase
{
    @Test
    public void testSingleQuerySingleResponseDefaultType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        testSingleQuerySingleResponse(Namespace.DEFAULT_BUCKET_TYPE);
    }
    
    @Test
    public void testSingleQuerySingleResponseTestType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        Assume.assumeTrue(testBucketType);
        testSingleQuerySingleResponse(bucketType.toString());
    }
    
    private void testSingleQuerySingleResponse(String bucketType) throws InterruptedException, ExecutionException
    {
        
        String indexName = "test_index";
        String keyBase = "my_key";
        String value = "value";
        Namespace ns = new Namespace(bucketType, bucketName.toString());
        
        setupIndexTestData(ns, indexName, keyBase, value);
        
        SecondaryIndexQueryOperation.Query query =
            new SecondaryIndexQueryOperation.Query.Builder(ns, BinaryValue.unsafeCreate((indexName + "_int").getBytes()))
                .withIndexKey(BinaryValue.unsafeCreate(String.valueOf(5L).getBytes()))
                .build();
        
        SecondaryIndexQueryOperation queryOp = 
            new SecondaryIndexQueryOperation.Builder(query)
                .build();
        
        cluster.execute(queryOp);
        SecondaryIndexQueryOperation.Response response = queryOp.get();
        
        assertEquals(1, response.getEntryList().size());
        assertFalse(response.getEntryList().get(0).hasIndexKey());
        assertEquals(response.getEntryList().get(0).getObjectKey().toString(), keyBase + "5");
        
        query =
            new SecondaryIndexQueryOperation.Query.Builder(ns, BinaryValue.unsafeCreate((indexName + "_int").getBytes()))
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
        assertEquals(response.getEntryList().get(0).getIndexKey(), BinaryValue.unsafeCreate("5".getBytes()));
        assertEquals(response.getEntryList().get(0).getObjectKey().toString(), keyBase + "5");
    }
    
    @Test
    public void testSingleQueryMultipleResponseDefaultType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        testSingleQueryMultipleResponse(Namespace.DEFAULT_BUCKET_TYPE);
    }
    
    @Test
    public void testSingleQueryMultipleResponseTestType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        Assume.assumeTrue(testBucketType);
        testSingleQueryMultipleResponse(bucketType.toString());
    }
    
    private void testSingleQueryMultipleResponse(String bucketType) throws InterruptedException, ExecutionException
    {
        
        String indexName = "test_index";
        String keyBase = "my_key";
        String value = "value";
        Namespace ns = new Namespace(bucketType, bucketName.toString());
        
        for (long i = 0; i < 100; i++)
        {
            RiakObject obj = new RiakObject().setValue(BinaryValue.create(value));

            obj.getIndexes().getIndex(LongIntIndex.named(indexName)).add(5L);
            Location location = new Location(ns, BinaryValue.unsafeCreate((keyBase + i).getBytes()));
            StoreOperation storeOp =
                new StoreOperation.Builder(location)
                    .withContent(obj)
                    .build();

            cluster.execute(storeOp);
            storeOp.get();
        }
        
        SecondaryIndexQueryOperation.Query query =
            new SecondaryIndexQueryOperation.Query.Builder(ns, BinaryValue.unsafeCreate((indexName + "_int").getBytes()))
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
        assertEquals(response.getEntryList().get(0).getObjectKey().toString(), keyBase + "0");
        
        query =
            new SecondaryIndexQueryOperation.Query.Builder(ns, BinaryValue.unsafeCreate((indexName + "_int").getBytes()))
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
        assertEquals(response.getEntryList().get(0).getIndexKey(), BinaryValue.unsafeCreate("5".getBytes()));
        assertEquals(response.getEntryList().get(0).getObjectKey().toString(), keyBase + "0");
        
    }
    
    @Test
    public void testRangeQueryDefaultType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        testRangeQuery(Namespace.DEFAULT_BUCKET_TYPE);
        
    }
    
    @Test
    public void testRangeQueryTestType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        Assume.assumeTrue(testBucketType);
        testRangeQuery(bucketType.toString());
        
    }
    
    private void testRangeQuery(String bucketType) throws InterruptedException, ExecutionException
    {
        
        String indexName = "test_index";
        String keyBase = "my_key";
        String value = "value";
        Namespace ns = new Namespace(bucketType, bucketName.toString());    
        
        setupIndexTestData(ns, indexName, keyBase, value);

            
        SecondaryIndexQueryOperation.Query query =
            new SecondaryIndexQueryOperation.Query.Builder(ns, BinaryValue.unsafeCreate((indexName + "_int").getBytes()))
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
        assertEquals(response.getEntryList().get(0).getObjectKey().toString(), keyBase + "5");
        
        query =
            new SecondaryIndexQueryOperation.Query.Builder(ns, BinaryValue.unsafeCreate((indexName + "_int").getBytes()))
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
        assertEquals(response.getEntryList().get(0).getIndexKey(), BinaryValue.unsafeCreate("5".getBytes()));
        assertEquals(response.getEntryList().get(0).getObjectKey().toString(), keyBase + "5");
    }

    @Test
    public void testNoSortWithNoPagingDefaultType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        testNoSortWithNoPaging(Namespace.DEFAULT_BUCKET_TYPE);
    }
    
    @Test
    public void testNoSortWithNoPagingTestType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        Assume.assumeTrue(testBucketType);
        testNoSortWithNoPaging(bucketType.toString());
    }
    
    private void testNoSortWithNoPaging(String bucketType) throws InterruptedException, ExecutionException
    {

        String indexName = "test_index_pagination";
        String value = "value";
        Namespace ns = new Namespace(bucketType, bucketName.toString());
        
        setupIndexTestData(ns, indexName, "", value);

        SecondaryIndexQueryOperation.Query query =
            new SecondaryIndexQueryOperation.Query.Builder(ns, BinaryValue.unsafeCreate((indexName + "_int").getBytes()))
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

    @Test
    public void testSortWithNoPagingDefaultType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        testSortWithNoPaging(Namespace.DEFAULT_BUCKET_TYPE);
    }
    
    @Test
    public void testSortWithNoPagingTestType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        Assume.assumeTrue(testBucketType);
        testSortWithNoPaging(bucketType.toString());
    }
    
    private void testSortWithNoPaging(String bucketType) throws InterruptedException, ExecutionException
    {

        String indexName = "test_index_pagination";
        String value = "value";
        Namespace ns = new Namespace(bucketType, bucketName.toString());

        setupIndexTestData(ns, indexName, "", value);

        
        SecondaryIndexQueryOperation.Query query =
            new SecondaryIndexQueryOperation.Query.Builder(ns, BinaryValue.unsafeCreate((indexName + "_int").getBytes()))
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

        AssertLongObjectsInOrder(response);
    }

    @Test
    public void testNoSortWithPagingDefaultType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        testNoSortWithPaging(Namespace.DEFAULT_BUCKET_TYPE);
    }
    
    @Test
    public void testNoSortWithPagingTestType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        Assume.assumeTrue(testBucketType);
        testNoSortWithPaging(bucketType.toString());
    }
    
    private void testNoSortWithPaging(String bucketType) throws InterruptedException, ExecutionException
    {

        String indexName = "test_index_pagination";
        String value = "value";
        Namespace ns = new Namespace(bucketType, bucketName.toString());
        
        setupIndexTestData(ns, indexName, "", value);

        try 
        {
            SecondaryIndexQueryOperation.Query query =
                new SecondaryIndexQueryOperation.Query.Builder(ns, BinaryValue.unsafeCreate((indexName + "_int").getBytes()))
                    .withRangeStart(BinaryValue.unsafeCreate(String.valueOf(0L).getBytes()))
                    .withRangeEnd(BinaryValue.unsafeCreate(String.valueOf(100L).getBytes()))
                    .withPaginationSort(false)
                    .withMaxResults(20)
                    .build();
            
            SecondaryIndexQueryOperation queryOp =
                new SecondaryIndexQueryOperation.Builder(query)
                        .build();

            fail("Didn't throw IllegalArgumentException");
        }
        catch(IllegalArgumentException ex) {
            assertNotNull(ex);
        }
    }

    @Test
    public void testSortWithPagingDefaultType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        testSortWithPaging(Namespace.DEFAULT_BUCKET_TYPE);
    }
    
    @Test
    public void testSortWithPagingTestType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        Assume.assumeTrue(testBucketType);
        testSortWithPaging(Namespace.DEFAULT_BUCKET_TYPE);
    }
    
    private void testSortWithPaging(String bucketType) throws InterruptedException, ExecutionException
    {
        String indexName = "test_index_pagination";
        String value = "value";
        Namespace ns = new Namespace(bucketType, bucketName.toString());

        setupIndexTestData(ns, indexName, "", value);
        
        SecondaryIndexQueryOperation.Query query =
                new SecondaryIndexQueryOperation.Query.Builder(ns, BinaryValue.unsafeCreate((indexName + "_int").getBytes()))
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

        AssertLongObjectsInOrder(response);
    }

    @Test
    public void testRegexTermFilterDefaultType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        testRegexTermFilter(Namespace.DEFAULT_BUCKET_TYPE);
    }
    
    @Test
    public void testRegexTermFilterTestType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        Assume.assumeTrue(testBucketType);
        testRegexTermFilter(bucketType.toString());
    }
    
    private void testRegexTermFilter(String bucketType) throws InterruptedException, ExecutionException
    {
        String indexName = "test_index_regex";
        String value = "value";
        Namespace ns = new Namespace(bucketType, bucketName.toString());
        
        for (long i = 0; i < 20; i++)
        {
            RiakObject obj = new RiakObject().setValue(BinaryValue.create(value));

            obj.getIndexes().getIndex(StringBinIndex.named(indexName)).add("foo" + String.format("%02d", i));

            
            Location location = new Location(ns, BinaryValue.unsafeCreate(Long.toString(i).getBytes()));
            StoreOperation storeOp =
                    new StoreOperation.Builder(location)
                            .withContent(obj)
                            .build();

            cluster.execute(storeOp);
            storeOp.get();
        }

        
        SecondaryIndexQueryOperation.Query query =
                new SecondaryIndexQueryOperation.Query.Builder(ns, BinaryValue.unsafeCreate((indexName + "_bin").getBytes()))
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
        assertEquals(response.getEntryList().get(0).getIndexKey(), BinaryValue.unsafeCreate("foo02".getBytes()));
        assertEquals(response.getEntryList().get(0).getObjectKey().toString(), "2");
        assertEquals(response.getEntryList().get(1).getIndexKey(), BinaryValue.unsafeCreate("foo12".getBytes()));
        assertEquals(response.getEntryList().get(1).getObjectKey().toString(), "12");
    }

    @Test
    public void testExceptionThrownWhenUsingRegexFilterOnIntIndexesDefaultType()
    {
        Assume.assumeTrue(test2i);
        testExceptionThrownWhenUsingRegexFilterOnIntIndexes(Namespace.DEFAULT_BUCKET_TYPE);
    }
    
    @Test
    public void testExceptionThrownWhenUsingRegexFilterOnIntIndexesTestType()
    {
        Assume.assumeTrue(test2i);
        Assume.assumeTrue(testBucketType);
        testExceptionThrownWhenUsingRegexFilterOnIntIndexes(bucketType.toString());
    }
    
    private void testExceptionThrownWhenUsingRegexFilterOnIntIndexes(String bucketType)
    {
        
        try {
            Namespace ns = new Namespace(bucketType, bucketName.toString());
            
            SecondaryIndexQueryOperation.Query query =
                new SecondaryIndexQueryOperation.Query.Builder(ns, BinaryValue.unsafeCreate(("foo_int").getBytes()))
                    .withRangeStart(BinaryValue.unsafeCreate("0".getBytes()))
                    .withRangeEnd(BinaryValue.unsafeCreate("100".getBytes()))
                    .withRegexTermFilter(BinaryValue.unsafeCreate("2".getBytes()))
                    .build();
            
            new SecondaryIndexQueryOperation.Builder(query)
                    .build();

            fail("Didn't throw IllegalArgumentException");
        }
        catch(IllegalArgumentException ex) {
            assertNotNull(ex);
        }
    }

    private void setupIndexTestData(Namespace ns, String indexName, String keyBase, String value)
            throws InterruptedException, ExecutionException
    {
        for (long i = 0; i < 100; i++)
        {
            RiakObject obj = new RiakObject().setValue(BinaryValue.create(value));

            obj.getIndexes().getIndex(LongIntIndex.named(indexName)).add(i);

            Location location = new Location(ns, keyBase + i);
            StoreOperation storeOp =
                    new StoreOperation.Builder(location)
                            .withContent(obj)
                            .build();

            cluster.execute(storeOp);
            storeOp.get();
        }
    }

    private void AssertLongObjectsInOrder(SecondaryIndexQueryOperation.Response response) {
        Long previousKey = Long.parseLong(response.getEntryList().get(0).getObjectKey().toString());
        for (int j = 1; j < response.getEntryList().size(); j++) {
            Long currentKey = Long.parseLong(response.getEntryList().get(j).getObjectKey().toString());
            assertTrue(previousKey <= currentKey);
            previousKey = currentKey;
        }
    }
}

