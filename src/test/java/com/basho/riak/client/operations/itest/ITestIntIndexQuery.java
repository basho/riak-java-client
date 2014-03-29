/*
 * Copyright 2014 Brian Roach <roach at basho dot com>.
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

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.annotations.RiakBucketName;
import com.basho.riak.client.annotations.RiakIndex;
import com.basho.riak.client.annotations.RiakKey;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.operations.indexes.IntIndexQuery;
import com.basho.riak.client.operations.kv.StoreValue;
import com.basho.riak.client.query.Location;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Assume;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class ITestIntIndexQuery extends ITestBase
{
    @Test
    public void simpleTest() throws InterruptedException, ExecutionException
    {
        //Assume.assumeTrue(test2i);
        
        RiakClient client = new RiakClient(cluster);
        
        IndexedPojo ip = new IndexedPojo();
        ip.key = "index_test_object_key";
        ip.bucketName = bucketName.toString();
        ip.indexKey = 123456L;
        ip.value = "My Object Value!";
        
        StoreValue sv = new StoreValue.Builder(ip).build();
        RiakFuture<StoreValue.Response, Location> svFuture = client.executeAsync(sv);
        
        svFuture.await();
        assertTrue(svFuture.isSuccess());
        
        Location loc = new Location(bucketName);
        
        IntIndexQuery iiq = 
            new IntIndexQuery.Builder(loc, "test_index", 123456L).withKeyAndIndex(true).build();
        IntIndexQuery.Response iResp = client.execute(iiq);
        
        assertTrue(iResp.hasEntries());
        IntIndexQuery.Response.Entry first = iResp.getEntries().iterator().next();
        assertEquals(ip.key, first.getRiakObjectLocation().getKey().toString());
        assertEquals(ip.indexKey, first.getIndexKey());
    }
    
    public static class IndexedPojo
    {
        @RiakKey
        public String key;
        
        @RiakBucketName
        public String bucketName;
        
        @RiakIndex(name="test_index")
        Long indexKey;
        
        public String value;
    }
}
