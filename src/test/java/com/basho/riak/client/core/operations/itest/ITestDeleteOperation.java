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

import com.basho.riak.client.core.operations.DeleteOperation;
import com.basho.riak.client.core.operations.FetchOperation;
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
 */
public class ITestDeleteOperation extends ITestAutoCleanupBase
{
    @Test
    public void testDeleteObjectDefaultType() throws InterruptedException, ExecutionException
    {
        testDeleteObject(Namespace.DEFAULT_BUCKET_TYPE);
    }
    
    @Test
    public void testDeleteObjectTestType() throws InterruptedException, ExecutionException
    {
        assumeTrue(testBucketType);
        testDeleteObject(bucketType.toString());
    }
    
    private void testDeleteObject(String bucketType) throws InterruptedException, ExecutionException
    {
        final BinaryValue key = BinaryValue.unsafeCreate("my_key".getBytes());
        final String value = "{\"value\":\"value\"}";
        
        RiakObject rObj = new RiakObject().setValue(BinaryValue.unsafeCreate(value.getBytes()));
        
        Location location = new Location(new Namespace(bucketType, bucketName.toString()), key);
        StoreOperation storeOp = 
            new StoreOperation.Builder(location)
                .withContent(rObj)
                .build(); 
        
        cluster.execute(storeOp);
        storeOp.get();
        
        FetchOperation fetchOp = 
            new FetchOperation.Builder(location).build();
                
        
        cluster.execute(fetchOp);
        FetchOperation.Response response = fetchOp.get();
        RiakObject rObj2 = response.getObjectList().get(0);
        
        assertEquals(rObj.getValue(), rObj2.getValue());
        
        DeleteOperation delOp =
            new DeleteOperation.Builder(location)
                .withVclock(rObj2.getVClock()).build();
        cluster.execute(delOp);
        delOp.get();
        
        fetchOp = 
            new FetchOperation.Builder(location).build();
        
        cluster.execute(fetchOp);
        response = fetchOp.get();
        assertTrue(response.isNotFound());
        assertTrue(response.getObjectList().isEmpty());
        
        
    }
}
