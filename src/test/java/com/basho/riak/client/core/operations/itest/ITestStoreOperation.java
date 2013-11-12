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
import com.basho.riak.client.query.KvResponse;
import com.basho.riak.client.util.ByteArrayWrapper;
import java.util.List;
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
    final private ByteArrayWrapper key = ByteArrayWrapper.unsafeCreate("my_key".getBytes());
    final private String value = "{\"value\":\"some value\"}";
    
    @Test
    public void testSimpleStore() throws InterruptedException, ExecutionException
    {
        
        RiakObject obj = new RiakObject().setValue(ByteArrayWrapper.create(value));
        
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
        RiakObject obj2 = fetchOp.get().getContent().get(0);
        
        assertEquals(obj.getValue(), obj2.getValue());
               
    }
    
    @Test
    public void testStoreWithVClockAndReturnbody() throws InterruptedException, ExecutionException
    {
        // Enable allow_multi, store a new item, then do a read/modify/write 
        // using the vclock
        ByteArrayWrapper bName = 
            ByteArrayWrapper.unsafeCreate((bucketName.toString() + "_1").getBytes());
        
        BucketProperties props = 
            new BucketProperties()
                .withAllowMulti(true);
        
        StoreBucketPropsOperation op = new StoreBucketPropsOperation(bName, props);
        cluster.execute(op);
        op.get();
        
        RiakObject obj = new RiakObject().setValue(ByteArrayWrapper.create(value));
        
        StoreOperation storeOp = 
            new StoreOperation.Builder(bName)
                .withKey(key)
                .withContent(obj)
                .withReturnBody(true)
                .build();
        
        cluster.execute(storeOp);
        KvResponse<List<RiakObject>> response = storeOp.get();
        obj = response.getContent().get(0);
        
        assertTrue(response.hasVClock());
        
        obj.setValue(ByteArrayWrapper.create("changed"));
        storeOp = new StoreOperation.Builder(bName)
                .withKey(key)
                .withContent(obj)
                .withVClock(response.getVClock())
                .withReturnBody(true)
                .build();
        
        cluster.execute(storeOp);
        response = storeOp.get();
        obj = response.getContent().get(0);
        
        assertEquals(obj.getValue().toString(), "changed");
        
        resetAndEmptyBucket(bName);
    }
    
}
