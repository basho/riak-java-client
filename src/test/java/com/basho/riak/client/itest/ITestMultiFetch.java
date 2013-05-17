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
package com.basho.riak.client.itest;

import static com.basho.riak.client.AllTests.emptyBucket;
import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.ConflictResolver;
import com.basho.riak.client.convert.RiakKey;
import com.basho.riak.client.query.MultiFetchFuture;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public abstract class ITestMultiFetch
{
    protected IRiakClient client;
    protected String bucketName;
    private String[] keys;
    private final ObjectMapper mapper = new ObjectMapper();

    @Before public void setUp() throws RiakException {
        this.client = getClient();
        this.bucketName = this.getClass().getName();
        emptyBucket(bucketName, client);
        Bucket b = client.fetchBucket(bucketName).execute();
        
        keys = new String[20];
        for (int i = 0; i < 20; i++)
        {
            b.store("key_" + String.valueOf(i), "{\"test\":" + String.valueOf(i) + "}").execute();
            keys[i] = "key_" + String.valueOf(i);
        }
    }

    /**
     * @return an {@link IRiakClient} implementation for the transport to be
     *         tested.
     */
    protected abstract IRiakClient getClient() throws RiakException;
    
    @Test public void multiFetchStrings() throws RiakException, InterruptedException, ExecutionException, IOException
    {
        Bucket b = client.fetchBucket(bucketName).execute();
        
        List<MultiFetchFuture<IRiakObject>> list = b.multiFetch(keys).execute();
        
        assertEquals(list.size(), keys.length);
        for (int i = 0; i < 20; i++)
        {
            MultiFetchFuture<IRiakObject> mff = list.get(i);
            String key = mff.getKey();
            assertEquals(key, keys[i]);
            String value = mff.get().getValueAsString();
            JsonNode json = mapper.readTree(value);
            assertEquals(json.get("test").asInt(), i);
        }
    }
    
    @Test public void multiFetchJsonDecode() throws RiakRetryFailedException, InterruptedException, ExecutionException
    {
        Bucket b = client.fetchBucket(bucketName).execute();
        
        List<MultiFetchFuture<MyPojo>> list = b.multiFetch(Arrays.asList(keys), MyPojo.class).execute();
        assertEquals(list.size(), keys.length);
        for (int i = 0; i < 20; i++)
        {
            MultiFetchFuture<MyPojo> mff = list.get(i);
            String key = mff.getKey();
            assertEquals(key, keys[i]);
            MyPojo value = mff.get();
            assertEquals(value.test, i);
        }
    }
    
    @Test public void multiFetchPojoKeys() throws RiakRetryFailedException, InterruptedException, ExecutionException
    {
        Bucket b = client.fetchBucket(bucketName).execute();
        List<MyPojo> list = new ArrayList<MyPojo>(20);
        for (int i = 0; i < 20; i++)
        {
            MyPojo mp = new MyPojo();
            mp.key = "key_" + String.valueOf(i);
            list.add(mp);
        }
        
        List<MultiFetchFuture<MyPojo>> list2 = b.multiFetch(list).execute();
        
        assertEquals(list.size(), list2.size());
        for (int i = 0; i < 20; i++)
        {
            MultiFetchFuture<MyPojo> mff = list2.get(i);
            String key = mff.getKey();
            assertEquals(key, keys[i]);
            MyPojo value = mff.get();
            assertEquals(value.test, i);
        }
    }
    
    @Test public void multiFetchSiblingsFail() throws RiakRetryFailedException, InterruptedException, ExecutionException, RiakException
    {
        Bucket b = client.createBucket(bucketName + "_2").allowSiblings(true).execute();
        
        for (int i = 0; i < 20; i++)
        {
            b.store("key_" + String.valueOf(i), "{\"test\":" + String.valueOf(i) + "}").execute();
        }
        
        // create a sibling
        b.store("key_0", "{\"test\":0}").withoutFetch().execute();
        
        // Only one of them will have a sibling - verify that, and that it throws 
        // an exception
        List<MultiFetchFuture<MyPojo>> list = b.multiFetch(Arrays.asList(keys), MyPojo.class).execute();
        assertEquals(list.size(), keys.length);
        boolean receivedException = false;
        for (int i = 0; i < 20; i++)
        {
            MultiFetchFuture<MyPojo> mff = list.get(i);
            String key = mff.getKey();
            assertEquals(key, keys[i]);

            try
            {
                MyPojo value = mff.get();
            }
            catch (ExecutionException ex)
            {
                if (key.equals("key_0"))
                {
                    receivedException = true;
                }
            }
        }
        
        emptyBucket(bucketName + "_2", client);
        assertTrue("Expected to receive conflict resolution exception for key_0",receivedException);
    }
    
    @Test public void multiFetchSiblingsResolve() throws RiakRetryFailedException, InterruptedException, ExecutionException, RiakException
    {
        Bucket b = client.createBucket(bucketName + "_2").allowSiblings(true).execute();
        
        for (int i = 0; i < 20; i++)
        {
            b.store("key_" + String.valueOf(i), "{\"test\":" + String.valueOf(i) + "}").execute();
        }
        
        // create a sibling
        b.store("key_0", "{\"test\":0}").withoutFetch().execute();
        
        // Use ConflictResolver
        List<MultiFetchFuture<MyPojo>> list = b.multiFetch(Arrays.asList(keys), MyPojo.class)
            .withResolver(new MyConflictResolver())
            .execute();
        
        assertEquals(list.size(), keys.length);
        for (int i = 0; i < 20; i++)
        {
            MultiFetchFuture<MyPojo> mff = list.get(i);
            String key = mff.getKey();
            assertEquals(key, keys[i]);
            MyPojo value = mff.get();
            assertEquals(value.test, i);
        }
        
        emptyBucket(bucketName + "_2", client);
    }
    
    
    
    private class MyConflictResolver implements ConflictResolver<MyPojo>
    {
        public MyPojo resolve(Collection<MyPojo> siblings)
        {
            return siblings.iterator().next();
        }
        
    }
    
    static class MyPojo
    {
        @RiakKey public String key;
        public int test;
        public MyPojo() {}
    }
}

