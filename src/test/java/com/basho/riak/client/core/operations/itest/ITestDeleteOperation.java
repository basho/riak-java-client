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

import com.basho.riak.client.DeleteMeta;
import com.basho.riak.client.convert.PassThroughConverter;
import com.basho.riak.client.core.operations.DeleteOperation;
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.ByteArrayWrapper;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestDeleteOperation extends ITestBase
{
    @Test
    public void testDeleteObject() throws InterruptedException, ExecutionException
    {
        final ByteArrayWrapper key = ByteArrayWrapper.unsafeCreate("my_key".getBytes());
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
                .withConverter(new PassThroughConverter());
        
        cluster.execute(fetchOp);
        RiakObject rObj2 = fetchOp.get();
        
        assertEquals(rObj.getValueAsString(), rObj2.getValueAsString());
        
        DeleteOperation delOp = 
            new DeleteOperation(bucketName, key)
            .withDeleteMeta(new DeleteMeta.Builder().vclock(rObj2.getVClock()).build());
        cluster.execute(delOp);
        delOp.get();
        
        fetchOp = 
            new FetchOperation<RiakObject>(bucketName, key).withConverter(new PassThroughConverter());
        
        cluster.execute(fetchOp);
        rObj2 = fetchOp.get();
        assertTrue(rObj2.isNotFound());
        assertNull(rObj2.getValueAsBytes());
        
        
    }
}
