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

package com.basho.riak.client.api.commands.indexes.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.annotations.RiakBucketName;
import com.basho.riak.client.api.annotations.RiakIndex;
import com.basho.riak.client.api.annotations.RiakKey;
import com.basho.riak.client.api.annotations.RiakVClock;
import com.basho.riak.client.api.cap.VClock;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.itest.ITestAutoCleanupBase;
import com.basho.riak.client.api.commands.indexes.BinIndexQuery;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
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
public class ITestBinIndexQuery extends ITestAutoCleanupBase
{
    @Test
    public void testMatchQuery() throws ExecutionException, InterruptedException
    {
        Assume.assumeTrue(test2i);

        RiakClient client = new RiakClient(cluster);

        IndexedPojo ip = new IndexedPojo();
        ip.key = "index_test_object_key";
        ip.bucketName = bucketName.toString();
        ip.indexKey = "index_test_index_key";
        ip.value = "My Object Value!";

        StoreValue sv = new StoreValue.Builder(ip).build();
        RiakFuture<StoreValue.Response, Location> svFuture = client.executeAsync(sv);

        svFuture.await();
        assertTrue(svFuture.isSuccess());

        IndexedPojo ip2 = new IndexedPojo();
        ip2.key = "index_test_object_key2";
        ip2.bucketName = bucketName.toString();
        ip2.indexKey = "index_test_index_key";
        ip2.value = "My Object Value!";

        sv = new StoreValue.Builder(ip2).build();
        svFuture = client.executeAsync(sv);

        svFuture.await();
        assertTrue(svFuture.isSuccess());

        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());

        BinIndexQuery biq  =
            new BinIndexQuery.Builder(ns, "test_index", "index_test_index_key").withKeyAndIndex(true).build();
        BinIndexQuery.Response iResp = client.execute(biq);

        assertTrue(iResp.hasEntries());
        assertEquals(2, iResp.getEntries().size());

        boolean found = false;
        for (BinIndexQuery.Response.Entry e : iResp.getEntries())
        {
            if (e.getRiakObjectLocation().getKey().toString().equals("index_test_object_key"))
            {
                found = true;
                assertEquals(ip.indexKey, e.getIndexKey());
            }
        }

        assertTrue(found);

    }

    @Test
    public void testRangeQuery() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);

        RiakClient client = new RiakClient(cluster);

        IndexedPojo ip = new IndexedPojo();
        ip.key = "index_test_object_key1";
        ip.bucketName = bucketName.toString();
        ip.indexKey = "index_test_index_key1";
        ip.value = "My Object Value!";

        StoreValue sv = new StoreValue.Builder(ip).build();
        RiakFuture<StoreValue.Response, Location> svFuture = client.executeAsync(sv);

        svFuture.await();
        assertTrue(svFuture.isSuccess());

        IndexedPojo ip2 = new IndexedPojo();
        ip2.key = "index_test_object_key2";
        ip2.bucketName = bucketName.toString();
        ip2.indexKey = "index_test_index_key2";
        ip2.value = "My Object Value!";

        sv = new StoreValue.Builder(ip2).build();
        svFuture = client.executeAsync(sv);

        svFuture.await();
        assertTrue(svFuture.isSuccess());

        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());
        BinIndexQuery biq  =
            new BinIndexQuery.Builder(ns, "test_index", "index_test_index_key0", "index_test_index_key9").withKeyAndIndex(true).build();
        BinIndexQuery.Response iResp = client.execute(biq);

        assertTrue(iResp.hasEntries());
        assertEquals(2, iResp.getEntries().size());

        boolean found = false;
        for (BinIndexQuery.Response.Entry e : iResp.getEntries())
        {
            if (e.getRiakObjectLocation().getKey().toString().equals("index_test_object_key1"))
            {
                found = true;
                assertEquals(ip.indexKey, e.getIndexKey());
            }
        }

        assertTrue(found);

    }

    public static class IndexedPojo
    {
        @RiakKey
        public String key;

        @RiakBucketName
        public String bucketName;

        @RiakIndex(name="test_index")
        String indexKey;

        @RiakVClock
        VClock vclock;

        public String value;
    }
}
