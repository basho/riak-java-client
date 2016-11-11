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
import com.basho.riak.client.api.commands.indexes.BucketIndexQuery;
import com.basho.riak.client.api.commands.indexes.KeyIndexQuery;
import com.basho.riak.client.api.commands.indexes.RawIndexQuery;
import com.basho.riak.client.api.commands.indexes.SecondaryIndexQuery.Type;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.indexes.IndexNames;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.stream.StreamSupport;

import static org.junit.Assert.*;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @author Alex Moore <amoore at basho dot com>
 */
public class ITestRawIndexQuery extends ITestBase
{
    private static String sharedBucket = "ITestRawIndexQuery";
    private static Namespace sharedNamespace = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, sharedBucket);
    private static RiakClient client = new RiakClient(cluster);

    @BeforeClass
    public static void Setup() throws ExecutionException, InterruptedException
    {
        for (long i = 0; i < 100; i++)
        {
            RiakObject obj = new RiakObject().setValue(BinaryValue.create("some_value"));

            Location location = new Location(sharedNamespace, "my_key" + i);
            StoreOperation storeOp =
                    new StoreOperation.Builder(location)
                            .withContent(obj)
                            .build();

            cluster.execute(storeOp);
            storeOp.get();
        }
    }

    @AfterClass
    public static void Teardown() throws ExecutionException, InterruptedException
    {
        resetAndEmptyBucket(BinaryValue.create(sharedBucket));
    }

    @Test
    public void simpleTest() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);

        setBucketNameToTestName();

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

        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());

        RawIndexQuery biq  =
            new RawIndexQuery.Builder(ns, "test_index", Type._BIN, indexKey).withKeyAndIndex(true).build();
        RawIndexQuery.Response iResp = client.execute(biq);

        assertTrue(iResp.hasEntries());
        RawIndexQuery.Response.Entry<BinaryValue> first = iResp.getEntries().iterator().next();
        assertEquals(ip.key, first.getRiakObjectLocation().getKey().toString());
        assertArrayEquals(ip.indexKey, first.getIndexKey().getValue());
    }

    @Test
    public void testKeyIndexHack() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);

        RawIndexQuery biq  =
            new RawIndexQuery.Builder(sharedNamespace,
                                      IndexNames.KEY,
                                      Type._KEY,
                                      BinaryValue.create("my_key10"),
                                      BinaryValue.create("my_key19"))
                .withKeyAndIndex(true).build();

        RawIndexQuery.Response iResp = client.execute(biq);
        assertTrue(iResp.hasEntries());
        assertEquals(iResp.getEntries().size(), 10);
    }

    @Test
    public void testKeyIndexQuery() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);

        KeyIndexQuery kq = new KeyIndexQuery.Builder(sharedNamespace, "my_key10", "my_key19").build();

        final RawIndexQuery.Response kqResp = client.execute(kq);
        assertTrue(kqResp.hasEntries());
        assertEquals(10, kqResp.getEntries().size());
    }

    @Test
    public void testBucketIndexHack() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);

        RawIndexQuery biq  =
            new RawIndexQuery.Builder(sharedNamespace,
                                      IndexNames.BUCKET, Type._BUCKET, BinaryValue.create(sharedBucket))
                .withKeyAndIndex(true).build();

        RawIndexQuery.Response iResp = client.execute(biq);
        assertTrue(iResp.hasEntries());
        assertEquals(100, iResp.getEntries().size());
    }

    @Test
    public void testBucketIndexQuery() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);

        BucketIndexQuery bq = new BucketIndexQuery.Builder(sharedNamespace).build();

        final BinIndexQuery.Response bqResp = client.execute(bq);
        assertTrue(bqResp.hasEntries());
        assertEquals(100, bqResp.getEntries().size());
    }

    @Test
    public void testBucketIndexQueryStreaming() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);

        BucketIndexQuery bq = new BucketIndexQuery.Builder(sharedNamespace).build();

        final RiakFuture<BinIndexQuery.Response, BinIndexQuery> indexResult =
                client.executeAsyncStreaming(bq, 100);

        final BinIndexQuery.Response streamingResponse = indexResult.get();

        assertTrue(streamingResponse.hasEntries());
        assertEquals(100, StreamSupport.stream(streamingResponse.spliterator(), false).count());

        // Assert everything was consumed
        assertFalse(streamingResponse.hasEntries());
    }

    @Test
    public void testIndexQueryStreamingContinuations() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);

        BucketIndexQuery bq = new BucketIndexQuery.Builder(sharedNamespace).withMaxResults(50).withPaginationSort(true).build();

        final RiakFuture<BinIndexQuery.Response, BinIndexQuery> indexResult =
                client.executeAsyncStreaming(bq, 100);

        final BinIndexQuery.Response streamingResponse = indexResult.get();

        assertTrue(streamingResponse.hasEntries());
        assertEquals(50, StreamSupport.stream(streamingResponse.spliterator(), false).count());
        assertTrue(streamingResponse.hasContinuation());
        assertNotNull(streamingResponse.getContinuation());

        final BinIndexQuery queryInfo = indexResult.getQueryInfo();
        assertEquals(bq, queryInfo);

        // Assert everything was consumed
        assertFalse(streamingResponse.hasEntries());
    }

    private static class IndexedPojo
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
