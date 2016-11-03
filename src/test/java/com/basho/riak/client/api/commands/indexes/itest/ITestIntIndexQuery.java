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
import com.basho.riak.client.api.StreamableRiakCommand;
import com.basho.riak.client.api.annotations.RiakBucketName;
import com.basho.riak.client.api.annotations.RiakIndex;
import com.basho.riak.client.api.annotations.RiakKey;
import com.basho.riak.client.api.commands.indexes.IntIndexQuery;
import com.basho.riak.client.api.commands.kv.FetchValue;
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
public class ITestIntIndexQuery extends ITestIndexBase
{
    private static final String OBJECT_KEY_BASE = "index_test_object_key";
    private static final String INDEX_NAME = "test_index";
    private static final Long DUP_INDEX_KEY = 123456L;
    private static RiakClient client = new RiakClient(cluster);

    @BeforeClass
    public static void setupData() throws InterruptedException, ExecutionException
    {
        createIndexedPojo(0, DUP_INDEX_KEY);
        createIndexedPojo(1, DUP_INDEX_KEY);
        createIndexedPojo(2, 25L);
    }

    @Test
    public void testFetchThings() throws ExecutionException, InterruptedException
    {
        FetchValue fv = new FetchValue.Builder(new Location(namespace, objectKey(0))).build();
        final FetchValue.Response execute = client.execute(fv);

        execute.getValues();
    }

    @Test
    public void testMatchQuery() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);

        IntIndexQuery indexQuery = new IntIndexQuery.Builder(namespace,
                                                             INDEX_NAME, DUP_INDEX_KEY).withKeyAndIndex(true).build();

        IntIndexQuery.Response indexResponse = client.execute(indexQuery);

        assertTrue(indexResponse.hasEntries());
        assertEquals(2, indexResponse.getEntries().size());

        assertFirstObjectFound(indexResponse);
    }

    @Test
    public void testRangeQuery() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);

        IntIndexQuery indexQuery = new IntIndexQuery.Builder(namespace,
                                                             INDEX_NAME,
                                                             Long.MIN_VALUE,
                                                             Long.MAX_VALUE).withKeyAndIndex(true).build();

        IntIndexQuery.Response indexResponse = client.execute(indexQuery);
        assertTrue(indexResponse.hasEntries());
        assertEquals(3, indexResponse.getEntries().size());

        assertFirstObjectFound(indexResponse);
    }

    @Test
    public void testStreamingRangeQuery() throws ExecutionException, InterruptedException
    {
        Assume.assumeTrue(test2i);

        IntIndexQuery indexQuery = new IntIndexQuery.Builder(namespace,
                                                             INDEX_NAME,
                                                             Long.MIN_VALUE,
                                                             Long.MAX_VALUE).withKeyAndIndex(true).build();

        final RiakFuture<IntIndexQuery.Response, IntIndexQuery> streamingFuture =
                client.executeAsyncStreaming((StreamableRiakCommand) indexQuery, 200);

        final IntIndexQuery.Response streamingResponse = streamingFuture.get();

        assertTrue(streamingResponse.hasEntries());

        final String expectedObjectKey = objectKey(1);
        boolean found = false;
        int size = 0;

        for (IntIndexQuery.Response.Entry e : streamingResponse)
        {
            size++;
            if (e.getRiakObjectLocation().getKey().toString().equals(expectedObjectKey))
            {
                found = true;
                assertEquals(DUP_INDEX_KEY, e.getIndexKey());
            }
        }

        assertTrue(found);
        assertEquals(3, size);
    }

    private void assertFirstObjectFound(IntIndexQuery.Response indexResponse)
    {
        boolean found = false;
        for (IntIndexQuery.Response.Entry e : indexResponse.getEntries())
        {
            if (e.getRiakObjectLocation().getKey().toString().equals(objectKey(1)))
            {
                found = true;
                assertEquals(DUP_INDEX_KEY, e.getIndexKey());
            }
        }

        assertTrue(found);
    }

    private static void createIndexedPojo(int keySuffix, long indexKey) throws InterruptedException
    {
        IndexedPojo ip = new IndexedPojo();
        ip.key = objectKey(keySuffix);
        ip.bucketName = namespace.getBucketNameAsString();
        ip.indexKey = indexKey;
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

    private static class IndexedPojo
    {
        @RiakKey
        public String key;

        @RiakBucketName
        public String bucketName;

        @RiakIndex(name = "test_index")
        Long indexKey;

        public String value;
    }
}
