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
import static com.basho.riak.client.core.operations.itest.ITestBase.bucketName;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.query.indexes.LongIntIndex;
import com.basho.riak.client.query.indexes.SecondaryIndexQueryResponse;
import com.basho.riak.client.util.ByteArrayWrapper;
import java.util.concurrent.ExecutionException;
import org.junit.Assume;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
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
        
        for (long i = 0; i < 100; i++)
        {
            RiakObject obj = new RiakObject().setValue(ByteArrayWrapper.create(value));
            
            obj.getIndexes().getIndex(new LongIntIndex.Name(indexName)).add(i);
            
            StoreOperation storeOp = 
                new StoreOperation.Builder(bucketName)
                    .withKey(ByteArrayWrapper.unsafeCreate((keyBase + i).getBytes()))
                    .withContent(obj)
                    .build();
            
            cluster.execute(storeOp);
            storeOp.get();
        }
        
        SecondaryIndexQueryOperation queryOp = 
            new SecondaryIndexQueryOperation.Builder(bucketName, ByteArrayWrapper.unsafeCreate((indexName + "_int").getBytes()))
                .withIndexKey(ByteArrayWrapper.unsafeCreate(String.valueOf(5L).getBytes()))
                .build();
        
        cluster.execute(queryOp);
        SecondaryIndexQueryResponse response = queryOp.get();
        
        assertEquals(1, response.size());
        assertFalse(response.get(0).hasIndexKey());
        assertEquals(response.get(0).getObjectKey().toString(), keyBase + "5");
        
        queryOp = 
            new SecondaryIndexQueryOperation.Builder(bucketName, ByteArrayWrapper.unsafeCreate((indexName + "_int").getBytes()))
                .withIndexKey(ByteArrayWrapper.unsafeCreate(String.valueOf(5L).getBytes()))
                .withReturnKeyAndIndex(true)
                .build();
        
        cluster.execute(queryOp);
        response = queryOp.get();
        
        assertEquals(1, response.size());
        assertTrue(response.get(0).hasIndexKey());
        assertEquals(response.get(0).getIndexKey(), ByteArrayWrapper.unsafeCreate("5".getBytes()));
        assertEquals(response.get(0).getObjectKey().toString(), keyBase + "5");
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
            RiakObject obj = new RiakObject().setValue(ByteArrayWrapper.create(value));
            
            obj.getIndexes().getIndex(new LongIntIndex.Name(indexName)).add(5L);
            
            StoreOperation storeOp = 
                new StoreOperation.Builder(bucketName)
                    .withKey(ByteArrayWrapper.unsafeCreate((keyBase + i).getBytes()))
                    .withContent(obj)
                    .build();
            
            cluster.execute(storeOp);
            storeOp.get();
        }
        
        SecondaryIndexQueryOperation queryOp = 
            new SecondaryIndexQueryOperation.Builder(bucketName, ByteArrayWrapper.unsafeCreate((indexName + "_int").getBytes()))
                .withIndexKey(ByteArrayWrapper.unsafeCreate(String.valueOf(5L).getBytes()))
                .build();
        
        cluster.execute(queryOp);
        SecondaryIndexQueryResponse response = queryOp.get();
        
        assertEquals(100, response.size());
        assertFalse(response.get(0).hasIndexKey());
        assertEquals(response.get(0).getObjectKey().toString(), keyBase + "0");
        
        queryOp = 
            new SecondaryIndexQueryOperation.Builder(bucketName, ByteArrayWrapper.unsafeCreate((indexName + "_int").getBytes()))
                .withIndexKey(ByteArrayWrapper.unsafeCreate(String.valueOf(5L).getBytes()))
                .withReturnKeyAndIndex(true)
                .build();
        
        cluster.execute(queryOp);
        response = queryOp.get();
        
        assertEquals(100, response.size());
        assertTrue(response.get(0).hasIndexKey());
        assertEquals(response.get(0).getIndexKey(), ByteArrayWrapper.unsafeCreate("5".getBytes()));
        assertEquals(response.get(0).getObjectKey().toString(), keyBase + "0");
        
    }
    
    @Test
    public void testRangeQuery() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        
        String indexName = "test_index";
        String keyBase = "my_key";
        String value = "value";
        
        for (long i = 0; i < 100; i++)
        {
            RiakObject obj = new RiakObject().setValue(ByteArrayWrapper.create(value));
            
            obj.getIndexes().getIndex(new LongIntIndex.Name(indexName)).add(i);
            
            StoreOperation storeOp = 
                new StoreOperation.Builder(bucketName)
                    .withKey(ByteArrayWrapper.unsafeCreate((keyBase + i).getBytes()))
                    .withContent(obj)
                    .build();
            
            cluster.execute(storeOp);
            storeOp.get();
        }
        
        SecondaryIndexQueryOperation queryOp = 
            new SecondaryIndexQueryOperation.Builder(bucketName, ByteArrayWrapper.unsafeCreate((indexName + "_int").getBytes()))
                    .withRangeStart(ByteArrayWrapper.unsafeCreate(String.valueOf(5L).getBytes()))
                    .withRangeEnd(ByteArrayWrapper.unsafeCreate(String.valueOf(20L).getBytes()))
                    .build();
        
        cluster.execute(queryOp);
        SecondaryIndexQueryResponse response = queryOp.get();
        
        assertEquals(16, response.size());
        assertFalse(response.get(0).hasIndexKey());
        assertEquals(response.get(0).getObjectKey().toString(), keyBase + "5");
        
        queryOp = 
            new SecondaryIndexQueryOperation.Builder(bucketName, ByteArrayWrapper.unsafeCreate((indexName + "_int").getBytes()))
                    .withRangeStart(ByteArrayWrapper.unsafeCreate(String.valueOf(5L).getBytes()))
                    .withRangeEnd(ByteArrayWrapper.unsafeCreate(String.valueOf(20L).getBytes()))
                    .withReturnKeyAndIndex(true)
                    .build();
        
        cluster.execute(queryOp);
        response = queryOp.get();
        assertEquals(16, response.size());
        assertTrue(response.get(0).hasIndexKey());
        assertEquals(response.get(0).getIndexKey(), ByteArrayWrapper.unsafeCreate("5".getBytes()));
        assertEquals(response.get(0).getObjectKey().toString(), keyBase + "5");
    }
    
    
}
