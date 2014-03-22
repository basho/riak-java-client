/*
 * Copyright 2014 Basho Technologies Inc.
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

package com.basho.riak.client.operations.itest;

import com.basho.riak.client.annotations.RiakBucketName;
import com.basho.riak.client.annotations.RiakKey;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.operations.kv.FetchValue;
import com.basho.riak.client.RiakClient;
import com.basho.riak.client.operations.kv.StoreOption;
import com.basho.riak.client.operations.kv.StoreValue;
import com.basho.riak.client.query.Location;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.*;
import org.junit.Assume;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestStoreValue extends ITestBase
{
    @Test
    public void simpleTest() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);
        
        Location loc = new Location(bucketName).setKey("test_store_key");
        Pojo pojo = new Pojo();
        pojo.value = "test store value";
        StoreValue sv = 
            new StoreValue.Builder(pojo).withLocation(loc)
                .withOption(StoreOption.RETURN_BODY, true)
                .build();
        
        StoreValue.Response resp = client.execute(sv);
        
        Pojo pojo2 = resp.getValue(Pojo.class);
        
        assertEquals(pojo.value, pojo2.value);
        
        FetchValue fv = new FetchValue.Builder(loc)
                            .build(); 
        
        FetchValue.Response fResp = client.execute(fv);
        
        assertEquals(pojo.value, fResp.getValue(Pojo.class).value);
        
    }
    
    
    
    @Test
    public void storeAnnotatedPojo() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);
        
        RiakAnnotatedPojo pojo = new RiakAnnotatedPojo();
        
        pojo.bucketType = "default";
        pojo.bucketName = bucketName.toString();
        pojo.key = "test_store_key_3";
        pojo.contentType = null; // converter will set
        pojo.value = "my value";
        
         StoreValue sv = 
            new StoreValue.Builder(pojo)
                .withOption(StoreOption.RETURN_BODY, true)
                .build();
        
        StoreValue.Response resp = client.execute(sv);
        
        RiakAnnotatedPojo rap = resp.getValue(RiakAnnotatedPojo.class);
        
        assertNotNull(rap.bucketName);
        assertEquals(pojo.bucketName, rap.bucketName);
        assertNotNull(rap.key);
        assertEquals(pojo.key, rap.key);
        assertNotNull(rap.bucketType);
        assertEquals(pojo.bucketType, rap.bucketType);
        assertNotNull(rap.contentType);
        assertEquals("application/json", rap.contentType);
        assertNotNull(rap.vclock);
        assertNotNull(rap.vtag);
        assertNotNull(rap.lastModified);
        assertNotNull(rap.value);
        assertFalse(rap.deleted);
        assertNotNull(rap.value);
        assertEquals(pojo.value, rap.value);
    }
    
    @Test
    public void storeAnnotatedPojoWithIndexes() throws ExecutionException, InterruptedException
    {
        Assume.assumeTrue(test2i);
        
        RiakClient client = new RiakClient(cluster);
        
        RiakAnnotatedPojo pojo = new RiakAnnotatedPojo();
        
        pojo.bucketType = "default";
        pojo.bucketName = bucketName.toString();
        pojo.key = "test_store_key_3";
        pojo.contentType = null; // converter will set
        pojo.value = "my value";
        
        Set<String> emailAddys = new HashSet<String>();
        emailAddys.add("roach@basho.com");
        
        pojo.emailIndx = emailAddys;
        pojo.userId = 1L;
        
        StoreValue sv = 
            new StoreValue.Builder(pojo)
                .withOption(StoreOption.RETURN_BODY, true)
                .build();
        
        StoreValue.Response resp = client.execute(sv);
        
        RiakAnnotatedPojo rap = resp.getValue(RiakAnnotatedPojo.class);
        
        assertTrue(rap.emailIndx.containsAll(emailAddys));
        assertEquals(rap.userId, pojo.userId);
        
        
    }
    
    public static class Pojo
    {
        @JsonProperty
        String value;
    }
    
}
