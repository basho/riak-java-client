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

import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.core.operations.StoreBucketPropsOperation;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.query.BucketProperties;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.ByteArrayWrapper;
import java.util.List;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestFetchOperation extends ITestBase
{
    
    @Test
    public void testFetchOpNotFound() throws InterruptedException, ExecutionException
    {
        final ByteArrayWrapper key = ByteArrayWrapper.unsafeCreate("my_key_1".getBytes());
        final String value = "{\"value\":\"value\"}";
        FetchOperation fetchOp = 
            new FetchOperation.Builder(bucketName, key).build();
                
        cluster.execute(fetchOp);
        FetchOperation.Response response = fetchOp.get();
        assertTrue(response.isNotFound());
        assertTrue(response.getObjectList().isEmpty());
        
    }
    
    @Test
    public void testFetchOpNoSiblings() throws InterruptedException, ExecutionException
    {
        final ByteArrayWrapper key = ByteArrayWrapper.unsafeCreate("my_key_2".getBytes());
        final String value = "{\"value\":\"value\"}";
        
        RiakObject rObj = new RiakObject().setValue(ByteArrayWrapper.create(value));
        
        StoreOperation storeOp = 
            new StoreOperation.Builder(bucketName)
                .withKey(key)
                .withContent(rObj)
                .build(); 
        
        cluster.execute(storeOp);
        storeOp.get();
        
        FetchOperation fetchOp = 
            new FetchOperation.Builder(bucketName, key).build();
        
        cluster.execute(fetchOp);
        FetchOperation.Response response = fetchOp.get();
        assertFalse(response.isNotFound());
        List<RiakObject> objectList = response.getObjectList();
        assertEquals(1, objectList.size());
        RiakObject ro = objectList.get(0);
        assertEquals(ro.getValue().toString(), value);
        
    }
    
    @Test
    public void testFetchOpWithSiblings() throws InterruptedException, ExecutionException
    {
        final ByteArrayWrapper key = ByteArrayWrapper.unsafeCreate("my_key_3".getBytes());
        final String value = "{\"value\":\"value\"}";
        
        BucketProperties props = 
            new BucketProperties()
                .withAllowMulti(true);
        
        StoreBucketPropsOperation op = new StoreBucketPropsOperation(bucketName, props);
        cluster.execute(op);
        op.get();
        
        RiakObject rObj = new RiakObject().setValue(ByteArrayWrapper.create(value));
        
        StoreOperation storeOp = 
            new StoreOperation.Builder(bucketName)
                .withKey(key)
                .withContent(rObj)
                .build();
        
        cluster.execute(storeOp);
        storeOp.get();
        
        storeOp = 
            new StoreOperation.Builder(bucketName)
                .withKey(key)
                .withContent(rObj)
                .build(); 
        
        cluster.execute(storeOp);
        storeOp.get();
        
        FetchOperation fetchOp = 
            new FetchOperation.Builder(bucketName, key).build();
                
        cluster.execute(fetchOp);
        FetchOperation.Response response = fetchOp.get();
        assertTrue(response.getObjectList().size() > 1);
        
        RiakObject ro = response.getObjectList().get(0);
        assertEquals(ro.getValue().toString(), value);
        
    }
    
}
