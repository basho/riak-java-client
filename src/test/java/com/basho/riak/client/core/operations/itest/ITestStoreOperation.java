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
import com.basho.riak.client.util.BinaryValue;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class ITestStoreOperation extends ITestBase
{
    final private BinaryValue key = BinaryValue.unsafeCreate("my_key".getBytes());
    final private String value = "{\"value\":\"some value\"}";
    
    @Test
    public void testSimpleStore() throws InterruptedException, ExecutionException
    {
        
        RiakObject obj = new RiakObject().setValue(BinaryValue.create(value));
        
        StoreOperation storeOp = 
            new StoreOperation.Builder(bucketName)
                .withKey(key)
                .withContent(obj)
                .build();
        
        cluster.execute(storeOp);
        storeOp.get();
        
        FetchOperation fetchOp = 
                new FetchOperation.Builder(bucketName, key).build();
                
        cluster.execute(fetchOp);
        RiakObject obj2 = fetchOp.get().getObjectList().get(0);
        
        assertEquals(obj.getValue(), obj2.getValue());
               
    }
    
    @Test
    public void testStoreWithVClockAndReturnbody() throws InterruptedException, ExecutionException
    {
        // Enable allow_multi, store a new item, then do a read/modify/write 
        // using the vclock
        BinaryValue bName = 
            BinaryValue.unsafeCreate((bucketName.toString() + "_1").getBytes());
        
        StoreBucketPropsOperation op = 
            new StoreBucketPropsOperation.Builder(bName)
                .withAllowMulti(true)
                .build();
        cluster.execute(op);
        op.get();
        
        RiakObject obj = new RiakObject().setValue(BinaryValue.create(value));
        
        StoreOperation storeOp = 
            new StoreOperation.Builder(bName)
                .withKey(key)
                .withContent(obj)
                .withReturnBody(true)
                .build();
        
        cluster.execute(storeOp);
        StoreOperation.Response response = storeOp.get();
        obj = response.getObjectList().get(0);
        
        assertTrue(response.hasVClock());
        
        obj.setValue(BinaryValue.create("changed"));
        storeOp = new StoreOperation.Builder(bName)
                .withKey(key)
                .withContent(obj)
                .withVClock(response.getVClock())
                .withReturnBody(true)
                .build();
        
        cluster.execute(storeOp);
        response = storeOp.get();
        obj = response.getObjectList().get(0);
        
        assertEquals(obj.getValue().toString(), "changed");
        
        resetAndEmptyBucket(bName);
    }
    
}
