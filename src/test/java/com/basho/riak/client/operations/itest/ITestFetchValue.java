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

import com.basho.riak.client.cap.ConflictResolver;
import com.basho.riak.client.cap.ConflictResolverFactory;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.operations.kv.FetchOption;
import com.basho.riak.client.operations.kv.FetchValue;
import com.basho.riak.client.RiakClient;
import com.basho.riak.client.operations.kv.StoreValue;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.query.indexes.LongIntIndex;
import com.basho.riak.client.query.indexes.StringBinIndex;
import com.basho.riak.client.util.BinaryValue;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestFetchValue extends ITestBase
{
    @Test
    public void simpleTest() 
    {
        try
        {
            RiakClient client = new RiakClient(cluster);
            Location loc = new Location(bucketName).setKey("test_fetch_key1");
            
            Pojo pojo = new Pojo();
            pojo.value = "test value";
            StoreValue sv =
                new StoreValue.Builder(pojo).withLocation(loc).build();
            
            StoreValue.Response resp = client.execute(sv);
            
            
            FetchValue fv = new FetchValue.Builder(loc).build();
            FetchValue.Response fResp = client.execute(fv);
            
            assertEquals(pojo.value, fResp.getValue(Pojo.class).value);
            
            RiakObject ro = fResp.getValue(RiakObject.class);
            assertNotNull(ro.getValue());
            assertEquals("{\"value\":\"test value\"}", ro.getValue().toString());
        }
        catch (ExecutionException ex)
        {
            System.out.println(ex.getCause().getCause());
        }
        catch (InterruptedException ex)
        {
            Logger.getLogger(ITestFetchValue.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    @Test
    public void notFound() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);
        Location loc = new Location(bucketName).setKey("test_fetch_key2");
        FetchValue fv = new FetchValue.Builder(loc).build();
        FetchValue.Response fResp = client.execute(fv);
        
        assertFalse(fResp.hasValues());
        assertTrue(fResp.isNotFound());
        assertNull(fResp.getValue(Pojo.class));
        RiakObject ro = fResp.getValue(RiakObject.class);
    }
    
    @Test
    public void ReproRiakTombstoneBehavior() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);
        Location loc = new Location(bucketName).setKey("test_fetch_key3");
        
        Pojo pojo = new Pojo();
        pojo.value = "test value";
        StoreValue sv = 
            new StoreValue.Builder(pojo).withLocation(loc).build();
        
        client.execute(sv);
        
        resetAndEmptyBucket(bucketName);
        
        client.execute(sv);
        
        FetchValue fv = new FetchValue.Builder(loc)
                            .withOption(FetchOption.DELETED_VCLOCK, false)
                            .build(); 
        
        FetchValue.Response fResp = client.execute(fv);
        
        assertEquals(2, fResp.getValues(RiakObject.class).size());
        
    }
    
    @Test
    public void resolveSiblings() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);
        Location loc = new Location(bucketName).setKey("test_fetch_key4");
        
        Pojo pojo = new Pojo();
        pojo.value = "test value";
        StoreValue sv = 
            new StoreValue.Builder(pojo).withLocation(loc).build();
        
        client.execute(sv);
        
        pojo.value = "Pick me!";
        
        sv = new StoreValue.Builder(pojo).withLocation(loc).build();
        
        client.execute(sv);
        
        ConflictResolverFactory.getInstance()
            .registerConflictResolver(Pojo.class, new MyResolver());
        
        FetchValue fv = new FetchValue.Builder(loc).build(); 
        
        FetchValue.Response fResp = client.execute(fv);
         
        assertEquals(pojo.value, fResp.getValue(Pojo.class).value);

        
    }
    
    @Test
    public void fetchAnnotatedPojo() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);
        Location loc = new Location(bucketName).setKey("test_fetch_key5");
        
        String jsonValue = "{\"value\":\"my value\"}";
        
        RiakObject ro = new RiakObject()
                        .setValue(BinaryValue.create(jsonValue))
                        .setContentType("application/json");
        
        StoreValue sv = new StoreValue.Builder(ro).withLocation(loc).build();
        client.execute(sv);
        
        FetchValue fv = new FetchValue.Builder(loc).build(); 
        FetchValue.Response resp = client.execute(fv);
        
        RiakAnnotatedPojo rap = resp.getValue(RiakAnnotatedPojo.class);
        
        assertNotNull(rap.bucketName);
        assertEquals(loc.getBucketNameAsString(), rap.bucketName);
        assertNotNull(rap.key);
        assertEquals(loc.getKeyAsString(), rap.key);
        assertNotNull(rap.bucketType);
        assertEquals(loc.getBucketTypeAsString(), rap.bucketType);
        assertNotNull(rap.contentType);
        assertEquals(ro.getContentType(), rap.contentType);
        assertNotNull(rap.vclock);
        assertNotNull(rap.vtag);
        assertNotNull(rap.lastModified);
        assertNotNull(rap.value);
        assertFalse(rap.deleted);
        assertNotNull(rap.value);
        assertEquals("my value", rap.value);
    }
    
    @Test
    public void fetchAnnotatedPojoWIthIndexes() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);
        Location loc = new Location(bucketName).setKey("test_fetch_key6");
        
        String jsonValue = "{\"value\":\"my value\"}";
        
        RiakObject ro = new RiakObject()
                        .setValue(BinaryValue.create(jsonValue))
                        .setContentType("application/json");
        
        ro.getIndexes().getIndex(StringBinIndex.named("email")).add("roach@basho.com");
        ro.getIndexes().getIndex(LongIntIndex.named("user_id")).add(1L);
        
        StoreValue sv = new StoreValue.Builder(ro).withLocation(loc).build();
        client.execute(sv);
        
        FetchValue fv = new FetchValue.Builder(loc).build(); 
        FetchValue.Response resp = client.execute(fv);
        
        RiakAnnotatedPojo rap = resp.getValue(RiakAnnotatedPojo.class);
        
        assertNotNull(rap.emailIndx);
        assertTrue(rap.emailIndx.contains("roach@basho.com"));
        assertEquals(rap.userId.longValue(), 1L);
        
        
        
    }
    
    public static class Pojo
    {
        @JsonProperty
        String value;
    }
    
    public static class MyResolver implements ConflictResolver<Pojo>
    {

        @Override
        public Pojo resolve(List<Pojo> objectList) throws UnresolvedConflictException
        {
            if (objectList.size() > 0)
            {
                for (Pojo p : objectList)
                {
                    if (p.value.equals("Pick me!"))
                    {
                        return p;
                    }
                }

                return objectList.get(0);
            }
            return null;
        }

    }
}
