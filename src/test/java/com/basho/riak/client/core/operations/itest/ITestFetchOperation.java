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

import com.basho.riak.client.cap.ConflictResolver;
import com.basho.riak.client.cap.DefaultResolver;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.PassThroughConverter;
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.core.operations.StoreOperation;
import static com.basho.riak.client.core.operations.itest.ITestBase.bucketName;
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
        FetchOperation<RiakObject> fetchOp = 
            new FetchOperation<RiakObject>(bucketName, key)
                .withConverter(new PassThroughConverter())
                .withResolver(new DefaultResolver<RiakObject>());
        
        cluster.execute(fetchOp);
        RiakObject ro = fetchOp.get();
        assertTrue(ro.isNotFound());
        
    }
    
    @Test
    public void testFetchOpNoSiblings() throws InterruptedException, ExecutionException
    {
        final ByteArrayWrapper key = ByteArrayWrapper.unsafeCreate("my_key_2".getBytes());
        final String value = "{\"value\":\"value\"}";
        
        RiakObject rObj = RiakObject.unsafeCreate(bucketName.getValue());
        rObj.setKey(key.unsafeGetValue()).setValue(value);
        
        StoreOperation<RiakObject> storeOp = 
            new StoreOperation<RiakObject>(bucketName)
                .withKey(key)
                .withContent(rObj)
                .withConverter(new PassThroughConverter()); 
        
        cluster.execute(storeOp);
        storeOp.get();
        
        FetchOperation<RiakObject> fetchOp = 
            new FetchOperation<RiakObject>(bucketName, key)
                .withConverter(new PassThroughConverter())
                .withResolver(new DefaultResolver<RiakObject>());
        
        cluster.execute(fetchOp);
        RiakObject ro = fetchOp.get();
        assertFalse(ro.isNotFound());
        assertEquals(ro.getValueAsString(), value);
        
    }
    
    @Test
    public void testFetchOpWithSiblings() throws InterruptedException, ExecutionException
    {
        final ByteArrayWrapper key = ByteArrayWrapper.unsafeCreate("my_key_3".getBytes());
        final String value = "{\"value\":\"value\"}";
        
        RiakObject rObj = RiakObject.unsafeCreate(bucketName.getValue());
        rObj.setKey(key.unsafeGetValue()).setValue(value);
        
        StoreOperation<RiakObject> storeOp = 
            new StoreOperation<RiakObject>(bucketName)
                .withKey(key)
                .withContent(rObj)
                .withConverter(new PassThroughConverter()); 
        
        cluster.execute(storeOp);
        storeOp.get();
        
        storeOp = 
            new StoreOperation<RiakObject>(bucketName)
                .withKey(key)
                .withContent(rObj)
                .withConverter(new PassThroughConverter()); 
        
        cluster.execute(storeOp);
        storeOp.get();
        
        ConflictResolver<RiakObject> resolver = new ConflictResolver() {

            @Override
            public Object resolve(List objectList) throws UnresolvedConflictException
            {
                assertTrue(objectList.size() > 1);
                return objectList.get(0);
            }
            
        };
        
        FetchOperation<RiakObject> fetchOp = 
            new FetchOperation<RiakObject>(bucketName, key)
                .withConverter(new PassThroughConverter())
                .withResolver(resolver);
        
        cluster.execute(fetchOp);
        RiakObject ro = fetchOp.get();
        assertFalse(ro.isNotFound());
        assertEquals(ro.getValueAsString(), value);
        
    }
    
}
