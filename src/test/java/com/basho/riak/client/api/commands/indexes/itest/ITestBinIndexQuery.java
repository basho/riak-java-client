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
import com.basho.riak.client.api.commands.indexes.BinIndexQuery;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.query.Location;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Brian Roach <roach at basho dot com>
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0
 */
public class ITestBinIndexQuery extends ITestIndexBase
{
    private static final String OBJECT_KEY_BASE = "index_test_object_key";
    private static final String INDEX_NAME = "test_index";
    private static final String INDEX_KEY_BASE = "index_test_index_key";
    private static RiakClient client = new RiakClient(cluster);

    @BeforeClass
    public static void setupData() throws InterruptedException
    {
        createIndexedPojo(0, 1);
        createIndexedPojo(1, 1);
        createIndexedPojo(2, 2);
    }

    @Test
    public void testMatchQuery() throws ExecutionException, InterruptedException
    {
        Assume.assumeTrue(test2i);

        BinIndexQuery indexQuery = new BinIndexQuery.Builder(namespace, INDEX_NAME, indexKey(1)).withKeyAndIndex(true)
                                                                                                .build();
        BinIndexQuery.Response indexResponse = client.execute(indexQuery);

        assertTrue(indexResponse.hasEntries());
        assertEquals(2, indexResponse.getEntries().size());

        boolean found = assertFirstObjectFound(indexResponse.getEntries());

        assertTrue(found);
    }

    @Test
    public void testRangeQuery() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);

        BinIndexQuery indexQuery = new BinIndexQuery.Builder(namespace,
                                                             INDEX_NAME,
                                                             indexKey(0),
                                                             indexKey(9)).withKeyAndIndex(true).build();
        BinIndexQuery.Response indexResponse = client.execute(indexQuery);

        assertTrue(indexResponse.hasEntries());
        assertEquals(3, indexResponse.getEntries().size());

        boolean found = assertFirstObjectFound(indexResponse.getEntries());

        assertTrue(found);
    }

    @Test
    public void testStreamingRangeQuery() throws ExecutionException, InterruptedException
    {
        Assume.assumeTrue(test2i);

        BinIndexQuery indexQuery = new BinIndexQuery.Builder(namespace,
                                                             INDEX_NAME,
                                                             indexKey(0),
                                                             indexKey(9)).withKeyAndIndex(true).build();

        final RiakFuture<BinIndexQuery.Response, BinIndexQuery> streamingFuture =
                client.executeAsyncStreaming(indexQuery, 200);

        final BinIndexQuery.Response streamingResponse = streamingFuture.get();

        assertTrue(streamingResponse.hasEntries());

        final String expectedObjectKey = objectKey(1);
        final String expectedIndexKey = indexKey(1);
        boolean found = false;
        int size = 0;

        for (BinIndexQuery.Response.Entry e : streamingResponse)
        {
            size++;
            if (e.getRiakObjectLocation().getKey().toString().equals(expectedObjectKey))
            {
                found = true;
                assertEquals(expectedIndexKey, e.getIndexKey());
            }
        }

        assertTrue(found);
        assertEquals(3, size);
    }

    private boolean assertFirstObjectFound(Iterable<BinIndexQuery.Response.Entry<String>> entries)
    {
        final String expectedObjectKey = objectKey(1);
        final String expectedIndexKey = indexKey(1);
        boolean found = false;

        for (BinIndexQuery.Response.Entry e : entries)
        {
            if (e.getRiakObjectLocation().getKey().toString().equals(expectedObjectKey))
            {
                found = true;
                assertEquals(expectedIndexKey, e.getIndexKey());
            }
        }
        return found;
    }

    private static void createIndexedPojo(int keySuffix, int indexSuffix) throws InterruptedException
    {
        IndexedPojo ip = new IndexedPojo();
        ip.key = objectKey(keySuffix);
        ip.bucketName = namespace.getBucketNameAsString();
        ip.indexKey = indexKey(indexSuffix);
        ip.value = "My Object Value!";

        StoreValue sv = new StoreValue.Builder(ip).build();
        RiakFuture<StoreValue.Response, Location> svFuture = client.executeAsync(sv);

        svFuture.await();
        assertTrue(svFuture.isSuccess());
    }

    private static String objectKey(int suffix)
    {
        return OBJECT_KEY_BASE + suffix;
    }

    private static String indexKey(int suffix)
    {
        return INDEX_KEY_BASE + suffix;
    }

    private static class IndexedPojo
    {
        @RiakKey
        public String key;

        @RiakBucketName
        public String bucketName;

        @RiakIndex(name = INDEX_NAME)
        String indexKey;

        public String value;
    }
}
