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
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class ITestStoreOperation extends ITestAutoCleanupBase
{
    final private BinaryValue key = BinaryValue.unsafeCreate("my_key".getBytes());
    final private String value = "{\"value\":\"some value\"}";
    
    @Test
    public void testSimpleStoreDefaultType() throws InterruptedException, ExecutionException
    {
        testSimpleStore(Namespace.DEFAULT_BUCKET_TYPE);
    }
    
    @Test
    public void testSimpleStoreTestType() throws InterruptedException, ExecutionException
    {
        assumeTrue(testBucketType);
        testSimpleStore(bucketType.toString());
    }
    
    private void testSimpleStore(String bucketType) throws InterruptedException, ExecutionException
    {
        
        RiakObject obj = new RiakObject().setValue(BinaryValue.create(value));
        Namespace ns = new Namespace(bucketType, bucketName.toString());
        Location location = new Location(ns, key);
        StoreOperation storeOp = 
            new StoreOperation.Builder(location)
                .withContent(obj)
                .build();
        
        cluster.execute(storeOp);
        storeOp.get();
        
        FetchOperation fetchOp = 
                new FetchOperation.Builder(location).build();
                
        cluster.execute(fetchOp);
        RiakObject obj2 = fetchOp.get().getObjectList().get(0);
        
        assertEquals(obj.getValue(), obj2.getValue());
               
    }
    
    @Test
    public void testStoreWithVClockAndReturnbodyDefaultType() throws InterruptedException, ExecutionException
    {
        assumeTrue(testBucketType);
        testStoreWithVClockAndReturnbody(bucketType.toString());
    }
    
    @Test
    public void testStoreWithVClockAndReturnbodyTestType() throws InterruptedException, ExecutionException
    {
        testStoreWithVClockAndReturnbody(Namespace.DEFAULT_BUCKET_TYPE);
    }
    
    private void testStoreWithVClockAndReturnbody(String bucketType) throws InterruptedException, ExecutionException
    {
        // Enable allow_multi, store a new item, then do a read/modify/write 
        // using the vclock
        
        Namespace ns = new Namespace(bucketType, bucketName.toString() + "_1");
        StoreBucketPropsOperation op = 
            new StoreBucketPropsOperation.Builder(ns)
                .withAllowMulti(true)
                .build();
        cluster.execute(op);
        op.get();
        
        RiakObject obj = new RiakObject().setValue(BinaryValue.create(value));
        Location location = new Location(ns, key);
       
        StoreOperation storeOp = 
            new StoreOperation.Builder(location)
                .withContent(obj)
                .withReturnBody(true)
                .build();
        
        cluster.execute(storeOp);
        StoreOperation.Response response = storeOp.get();
        obj = response.getObjectList().get(0);
        
        assertNotNull(obj.getVClock());
        
        obj.setValue(BinaryValue.create("changed"));

        storeOp = new StoreOperation.Builder(location)
                .withContent(obj)
                .withReturnBody(true)
                .build();
        
        cluster.execute(storeOp);
        response = storeOp.get();
        obj = response.getObjectList().get(0);
        
        assertEquals(obj.getValue().toString(), "changed");
        
        resetAndEmptyBucket(ns);
    }
    
}
