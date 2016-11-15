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

package com.basho.riak.client.api.commands.indexes.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.annotations.RiakBucketName;
import com.basho.riak.client.api.annotations.RiakIndex;
import com.basho.riak.client.api.annotations.RiakKey;
import com.basho.riak.client.api.commands.indexes.BigIntIndexQuery;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigInteger;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Brian Roach <roach at basho dot com>
 * @author Alex Moore <amoore at basho dot com>
 */
public class ITestBigIntIndexQuery extends ITestIndexBase
{
    private static final String OBJECT_KEY_BASE = "index_test_object_key";
    private static final String INDEX_NAME = "test_index";
    private static final BigInteger INDEX_ENTRY = new BigInteger("91234567890123456789012345678901234567890");
    private static final BigInteger INDEX_ENTRY2 = new BigInteger("91234567890123456789012345678901234567898");
    private static final RiakClient client = new RiakClient(cluster);

    @BeforeClass
    public static void setupData() throws InterruptedException
    {
        createIndexedPojo(0, INDEX_ENTRY);
        createIndexedPojo(1, INDEX_ENTRY);
        createIndexedPojo(2, INDEX_ENTRY2);
    }

    @Test
    public void testMatchQuery() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);

        RiakClient client = new RiakClient(cluster);

        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());

        BigIntIndexQuery indexQuery = new BigIntIndexQuery.Builder(ns, INDEX_NAME, INDEX_ENTRY).withKeyAndIndex(true)
                                                                                               .build();
        BigIntIndexQuery.Response indexResponse = client.execute(indexQuery);

        assertTrue(indexResponse.hasEntries());
        assertEquals(2, indexResponse.getEntries().size());

        assertFirstObjectFound(indexResponse);
    }

    @Test
    public void testRangeQuery() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);

        BigIntIndexQuery indexQuery = new BigIntIndexQuery.Builder(namespace,
                                                                   INDEX_NAME,
                                                                   INDEX_ENTRY,
                                                                   INDEX_ENTRY2).withKeyAndIndex(true).build();

        BigIntIndexQuery.Response indexResponse = client.execute(indexQuery);
        assertTrue(indexResponse.hasEntries());
        assertEquals(3, indexResponse.getEntries().size());

        assertFirstObjectFound(indexResponse);
    }

    @Test
    public void testStreamingRangeQuery() throws ExecutionException, InterruptedException
    {
        Assume.assumeTrue(test2i);

        BigIntIndexQuery indexQuery = new BigIntIndexQuery.Builder(namespace,
                                                                   INDEX_NAME,
                                                                   INDEX_ENTRY,
                                                                   INDEX_ENTRY2).withKeyAndIndex(true).build();

        final RiakFuture<BigIntIndexQuery.Response, BigIntIndexQuery> streamingFuture =
                client.executeAsyncStreaming(indexQuery, 200);

        final BigIntIndexQuery.Response streamingResponse = streamingFuture.get();

        assertTrue(streamingResponse.hasEntries());
        assertTrue(streamingResponse.getEntries().isEmpty());

        final String expectedObjectKey = objectKey(1);
        boolean found = false;
        int size = 0;

        for (BigIntIndexQuery.Response.Entry e : streamingResponse)
        {
            size++;
            if (e.getRiakObjectLocation().getKey().toString().equals(expectedObjectKey))
            {
                found = true;
                assertEquals(INDEX_ENTRY, e.getIndexKey());
            }
        }

        assertTrue(found);
        assertEquals(3, size);
    }

    private boolean assertFirstObjectFound(BigIntIndexQuery.Response response)
    {
        final String expectedObjectKey = objectKey(1);
        boolean found = false;

        for (BigIntIndexQuery.Response.Entry e : response.getEntries())
        {
            if (e.getRiakObjectLocation().getKey().toString().equals(expectedObjectKey))
            {
                found = true;
                assertEquals(INDEX_ENTRY, e.getIndexKey());
            }
        }
        return found;
    }

    private static void createIndexedPojo(int keySuffix, BigInteger indexValue) throws InterruptedException
    {
        IndexedPojo ip = new IndexedPojo();
        ip.key = objectKey(keySuffix);
        ip.bucketName = namespace.getBucketNameAsString();
        ip.indexKey = indexValue;
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

        @RiakIndex(name = INDEX_NAME)
        BigInteger indexKey;

        public String value;
    }
}
