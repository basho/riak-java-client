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
import com.basho.riak.client.operations.indexes.RawIndexQuery;
import com.basho.riak.client.operations.indexes.SecondaryIndexQuery.Type;
import com.basho.riak.client.operations.kv.StoreValue;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.util.BinaryValue;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Assume;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestRawIndexQuery extends ITestBase
{
    @Test
    public void simpleTest() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        
        RiakClient client = new RiakClient(cluster);
        
        BinaryValue indexKey = BinaryValue.create("index_test_index_key");
        
        IndexedPojo ip = new IndexedPojo();
        ip.key = "index_test_object_key";
        ip.bucketName = bucketName.toString();
        ip.indexKey = indexKey.getValue();
        ip.value = "My Object Value!";
        
        StoreValue sv = new StoreValue.Builder(ip).build();
        RiakFuture<StoreValue.Response, Location> svFuture = client.executeAsync(sv);
        
        svFuture.await();
        assertTrue(svFuture.isSuccess());
        
        Location loc = new Location(bucketName);
        
        RawIndexQuery biq  =
            new RawIndexQuery.Builder(loc, "test_index", Type._BIN, indexKey).withKeyAndIndex(true).build();
        RawIndexQuery.Response iResp = client.execute(biq);
        
        assertTrue(iResp.hasEntries());
        RawIndexQuery.Response.Entry first = iResp.getEntries().iterator().next();
        assertEquals(ip.key, first.getRiakObjectLocation().getKey().toString());
        assertArrayEquals(ip.indexKey, first.getIndexKey().getValue());
        
    }
    
    public static class IndexedPojo
    {
        @RiakKey
        public String key;
        
        @RiakBucketName
        public String bucketName;
        
        @RiakIndex(name="test_index_bin")
        byte[] indexKey;
        
        public String value;
    }
}
