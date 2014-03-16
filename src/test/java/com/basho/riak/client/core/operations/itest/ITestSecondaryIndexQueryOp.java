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
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.query.indexes.LongIntIndex;
import com.basho.riak.client.query.indexes.StringBinIndex;
import com.basho.riak.client.util.BinaryValue;

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
    public void testSingleQuerySingleResponse() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        
        String indexName = "test_index";
        String keyBase = "my_key";
        String value = "value";

        SetupIndexTestData(indexName, keyBase, value);

        SecondaryIndexQueryOperation queryOp = 
            new SecondaryIndexQueryOperation.Builder(bucketName, BinaryValue.unsafeCreate((indexName + "_int").getBytes()))
                .withIndexKey(BinaryValue.unsafeCreate(String.valueOf(5L).getBytes()))
                .build();
        
        cluster.execute(queryOp);
        SecondaryIndexQueryOperation.Response response = queryOp.get();
        
        assertEquals(1, response.getEntryList().size());
        assertFalse(response.getEntryList().get(0).hasIndexKey());
        assertEquals(response.getEntryList().get(0).getObjectKey().toString(), keyBase + "5");
        
        queryOp = 
            new SecondaryIndexQueryOperation.Builder(bucketName, BinaryValue.unsafeCreate((indexName + "_int").getBytes()))
                .withIndexKey(BinaryValue.unsafeCreate(String.valueOf(5L).getBytes()))
                .withReturnKeyAndIndex(true)
                .build();
        
        cluster.execute(queryOp);
        response = queryOp.get();
        
        assertEquals(1, response.getEntryList().size());
        assertTrue(response.getEntryList().get(0).hasIndexKey());
        assertEquals(response.getEntryList().get(0).getIndexKey(), BinaryValue.unsafeCreate("5".getBytes()));
        assertEquals(response.getEntryList().get(0).getObjectKey().toString(), keyBase + "5");
    }
    
    @Test
    public void testSingleQueryMultipleResponse() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        
        String indexName = "test_index";
        String keyBase = "my_key";
        String value = "value";
        
        for (long i = 0; i < 100; i++)
        {
            RiakObject obj = new RiakObject().setValue(BinaryValue.create(value));

            obj.getIndexes().getIndex(new LongIntIndex.Name(indexName)).add(5L);

            Location location = new Location(bucketName).setKey(BinaryValue.unsafeCreate((keyBase + i).getBytes()));
            StoreOperation storeOp =
                new StoreOperation.Builder(location)
                    .withContent(obj)
                    .build();

            cluster.execute(storeOp);
            storeOp.get();
        }
        
        SecondaryIndexQueryOperation queryOp = 
            new SecondaryIndexQueryOperation.Builder(bucketName, BinaryValue.unsafeCreate((indexName + "_int").getBytes()))
                .withIndexKey(BinaryValue.unsafeCreate(String.valueOf(5L).getBytes()))
                .withPaginationSort(true)
                .build();
        
        cluster.execute(queryOp);
        SecondaryIndexQueryOperation.Response response = queryOp.get();
        
        assertEquals(100, response.getEntryList().size());
        assertFalse(response.getEntryList().get(0).hasIndexKey());
        assertEquals(response.getEntryList().get(0).getObjectKey().toString(), keyBase + "0");
        
        queryOp = 
            new SecondaryIndexQueryOperation.Builder(bucketName, BinaryValue.unsafeCreate((indexName + "_int").getBytes()))
                .withIndexKey(BinaryValue.unsafeCreate(String.valueOf(5L).getBytes()))
                .withReturnKeyAndIndex(true)
                .withPaginationSort(true)
                .build();
        
        cluster.execute(queryOp);
        response = queryOp.get();
        
        assertEquals(100, response.getEntryList().size());
        assertTrue(response.getEntryList().get(0).hasIndexKey());
        assertEquals(response.getEntryList().get(0).getIndexKey(), BinaryValue.unsafeCreate("5".getBytes()));
        assertEquals(response.getEntryList().get(0).getObjectKey().toString(), keyBase + "0");
        
    }
    
    @Test
    public void testRangeQuery() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        
        String indexName = "test_index";
        String keyBase = "my_key";
        String value = "value";

        SetupIndexTestData(indexName, keyBase, value);

        SecondaryIndexQueryOperation queryOp = 
            new SecondaryIndexQueryOperation.Builder(bucketName, BinaryValue.unsafeCreate((indexName + "_int").getBytes()))
                    .withRangeStart(BinaryValue.unsafeCreate(String.valueOf(5L).getBytes()))
                    .withRangeEnd(BinaryValue.unsafeCreate(String.valueOf(20L).getBytes()))
                    .withPaginationSort(true)
                    .build();
        
        cluster.execute(queryOp);
        SecondaryIndexQueryOperation.Response response = queryOp.get();
        
        assertEquals(16, response.getEntryList().size());
        assertFalse(response.getEntryList().get(0).hasIndexKey());
        assertEquals(response.getEntryList().get(0).getObjectKey().toString(), keyBase + "5");
        
        queryOp = 
            new SecondaryIndexQueryOperation.Builder(bucketName, BinaryValue.unsafeCreate((indexName + "_int").getBytes()))
                    .withRangeStart(BinaryValue.unsafeCreate(String.valueOf(5L).getBytes()))
                    .withRangeEnd(BinaryValue.unsafeCreate(String.valueOf(20L).getBytes()))
                    .withReturnKeyAndIndex(true)
                    .withPaginationSort(true)
                    .build();
        
        cluster.execute(queryOp);
        response = queryOp.get();
        assertEquals(16, response.getEntryList().size());
        assertTrue(response.getEntryList().get(0).hasIndexKey());
        assertEquals(response.getEntryList().get(0).getIndexKey(), BinaryValue.unsafeCreate("5".getBytes()));
        assertEquals(response.getEntryList().get(0).getObjectKey().toString(), keyBase + "5");
    }

    @Test
    public void testNoSortWithNoPaging() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);

        String indexName = "test_index_pagination";
        String value = "value";

        SetupIndexTestData(indexName, "", value);

        SecondaryIndexQueryOperation queryOp =
                new SecondaryIndexQueryOperation.Builder(bucketName, BinaryValue.unsafeCreate((indexName + "_int").getBytes()))
                        .withRangeStart(BinaryValue.unsafeCreate(String.valueOf(0L).getBytes()))
                        .withRangeEnd(BinaryValue.unsafeCreate(String.valueOf(100L).getBytes()))
                        .withPaginationSort(false)
                        .build();

        cluster.execute(queryOp);
        SecondaryIndexQueryOperation.Response response = queryOp.get();

        assertEquals(100, response.getEntryList().size());
    }

    @Test
    public void testSortWithNoPaging() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);

        String indexName = "test_index_pagination";
        String value = "value";

        SetupIndexTestData(indexName, "", value);

        SecondaryIndexQueryOperation queryOp =
                new SecondaryIndexQueryOperation.Builder(bucketName, BinaryValue.unsafeCreate((indexName + "_int").getBytes()))
                        .withRangeStart(BinaryValue.unsafeCreate(String.valueOf(0L).getBytes()))
                        .withRangeEnd(BinaryValue.unsafeCreate(String.valueOf(100L).getBytes()))
                        .withPaginationSort(true)
                        .build();

        cluster.execute(queryOp);
        SecondaryIndexQueryOperation.Response response = queryOp.get();

        assertEquals(100, response.getEntryList().size());

        AssertLongObjectsInOrder(response);
    }

    @Test
    public void testNoSortWithPaging() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);

        String indexName = "test_index_pagination";
        String value = "value";

        SetupIndexTestData(indexName, "", value);

        try {
            SecondaryIndexQueryOperation queryOp =
                new SecondaryIndexQueryOperation.Builder(bucketName, BinaryValue.unsafeCreate((indexName + "_int").getBytes()))
                        .withRangeStart(BinaryValue.unsafeCreate(String.valueOf(0L).getBytes()))
                        .withRangeEnd(BinaryValue.unsafeCreate(String.valueOf(100L).getBytes()))
                        .withPaginationSort(false)
                        .withMaxResults(20)
                        .build();

            fail("Didn't throw IllegalArgumentException");
        }
        catch(IllegalArgumentException ex) {
            assertNotNull(ex);
        }
    }

    @Test
    public void testSortWithPaging() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);

        String indexName = "test_index_pagination";
        String value = "value";

        SetupIndexTestData(indexName, "", value);

        SecondaryIndexQueryOperation queryOp =
                new SecondaryIndexQueryOperation.Builder(bucketName, BinaryValue.unsafeCreate((indexName + "_int").getBytes()))
                        .withRangeStart(BinaryValue.unsafeCreate(String.valueOf(0L).getBytes()))
                        .withRangeEnd(BinaryValue.unsafeCreate(String.valueOf(100L).getBytes()))
                        .withPaginationSort(true)
                        .withMaxResults(20)
                        .build();

        cluster.execute(queryOp);
        SecondaryIndexQueryOperation.Response response = queryOp.get();

        assertEquals(20, response.getEntryList().size());

        AssertLongObjectsInOrder(response);
    }

    @Test
    public void testRegexTermFilter() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);

        String indexName = "test_index_regex";
        String value = "value";

        for (long i = 0; i < 20; i++)
        {
            RiakObject obj = new RiakObject().setValue(BinaryValue.create(value));

            obj.getIndexes().getIndex(new StringBinIndex.Name(indexName)).add("foo" + String.format("%02d", i));

            Location location = new Location(bucketName).setKey(BinaryValue.unsafeCreate(Long.toString(i).getBytes()));
            StoreOperation storeOp =
                    new StoreOperation.Builder(location)
                            .withContent(obj)
                            .build();

            cluster.execute(storeOp);
            storeOp.get();
        }

        SecondaryIndexQueryOperation queryOp =
                new SecondaryIndexQueryOperation.Builder(bucketName, BinaryValue.unsafeCreate((indexName + "_bin").getBytes()))
                        .withRangeStart(BinaryValue.unsafeCreate("foo00".getBytes()))
                        .withRangeEnd(BinaryValue.unsafeCreate("foo19".getBytes()))
                        .withRegexTermFilter(BinaryValue.unsafeCreate("2".getBytes()))
                        .withReturnKeyAndIndex(true)
                        .withPaginationSort(true)
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
    public void testExceptionThrownWhenUsingRegexFilterOnIntIndexes()
    {
        Assume.assumeTrue(test2i);
        
        try {
            new SecondaryIndexQueryOperation.Builder(bucketName, BinaryValue.unsafeCreate(("foo_int").getBytes()))
                    .withRangeStart(BinaryValue.unsafeCreate("0".getBytes()))
                    .withRangeEnd(BinaryValue.unsafeCreate("100".getBytes()))
                    .withRegexTermFilter(BinaryValue.unsafeCreate("2".getBytes()))
                    .build();

            fail("Didn't throw IllegalArgumentException");
        }
        catch(IllegalArgumentException ex) {
            assertNotNull(ex);
        }
    }

    private void SetupIndexTestData(String indexName, String keyBase, String value)
            throws InterruptedException, ExecutionException
    {
        for (long i = 0; i < 100; i++)
        {
            RiakObject obj = new RiakObject().setValue(BinaryValue.create(value));

            obj.getIndexes().getIndex(new LongIntIndex.Name(indexName)).add(i);

            Location location = new Location(bucketName).setKey(BinaryValue.unsafeCreate((keyBase + i).getBytes()));
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

