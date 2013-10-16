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

import com.basho.riak.client.StoreMeta;
import com.basho.riak.client.cap.DefaultResolver;
import com.basho.riak.client.convert.PassThroughConverter;
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.core.operations.StoreBucketPropsOperation;
import com.basho.riak.client.core.operations.StoreOperation;
import static com.basho.riak.client.core.operations.itest.ITestBase.cluster;
import com.basho.riak.client.query.BucketProperties;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.ByteArrayWrapper;
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
        
        RiakObject obj = RiakObject.create(bucketName.unsafeGetValue())
                            .setKey(key.unsafeGetValue())
                            .setValue(value);
        
        StoreOperation<RiakObject> storeOp = 
            new StoreOperation<RiakObject>(bucketName)
                .withKey(key)
                .withContent(obj)
                .withConverter(new PassThroughConverter());
        
        cluster.execute(storeOp);
        storeOp.get();
        
        FetchOperation<RiakObject> fetchOp = 
                new FetchOperation<RiakObject>(bucketName, key)
                    .withConverter(new PassThroughConverter())
                    .withResolver(new DefaultResolver<RiakObject>());
                
        cluster.execute(fetchOp);
        RiakObject obj2 = fetchOp.get();
        
        assertEquals(obj.getValueAsString(), obj.getValueAsString());
               
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
        
        RiakObject obj = RiakObject.create(bName.unsafeGetValue())
                            .setKey(key.unsafeGetValue())
                            .setValue(value);
        
        StoreOperation<RiakObject> storeOp = 
            new StoreOperation<RiakObject>(bName)
                .withKey(key)
                .withContent(obj)
                .withConverter(new PassThroughConverter())
                .withStoreMeta(new StoreMeta.Builder().returnBody(true).build());
        
        cluster.execute(storeOp);
        obj = storeOp.get();
        
        assertNotNull(obj.getVClock());
        System.out.println("Vclock: " + obj.getVClock());
        
        obj.setValue("changed");
        storeOp = new StoreOperation<RiakObject>(bName)
                .withKey(key)
                .withContent(obj)
                .withConverter(new PassThroughConverter())
                .withStoreMeta(new StoreMeta.Builder().returnBody(true).build());
        
        cluster.execute(storeOp);
        obj = storeOp.get();
        
        assertEquals(obj.getValueAsString(), "changed");
        
        resetAndEmptyBucket(bName);
    }
    
}
