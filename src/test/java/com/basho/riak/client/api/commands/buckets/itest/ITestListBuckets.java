/*
 * Copyright 2016 Basho Technologies, Inc.
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

package com.basho.riak.client.api.commands.buckets.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.buckets.ListBuckets;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * @author empovit
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class ITestListBuckets extends ITestBase
{
    private static final RiakClient client = new RiakClient(cluster);
    private static final String bucketName = "ITestListBuckets";
    private static final Namespace defaultNamespace = new Namespace(bucketName);
    private static final Namespace typedNamespace = new Namespace(bucketType.toString(), bucketName);

    @BeforeClass
    public static void setup() throws ExecutionException, InterruptedException
    {
        storeTestObject(defaultNamespace);
        if (testBucketType)
        {
            storeTestObject(typedNamespace);
        }
    }

    @AfterClass
    public static void cleanup() throws ExecutionException, InterruptedException
    {
        resetAndEmptyBucket(defaultNamespace);
        if (testBucketType)
        {
            resetAndEmptyBucket(typedNamespace);
        }
    }

    @Test
    public void testListBucketsDefaultType() throws InterruptedException, ExecutionException
    {
        testListBuckets(defaultNamespace);
    }

    @Test
    public void testListBucketsTestType() throws InterruptedException, ExecutionException
    {
        assumeTrue(testBucketType);
        testListBuckets(typedNamespace);
    }

    @Test
    public void testListBucketsStreamingTestType() throws InterruptedException, ExecutionException
    {
        assumeTrue(testBucketType);
        testListBucketsStreaming(typedNamespace);
    }

    private void testListBuckets(Namespace namespace) throws InterruptedException, ExecutionException
    {
        ListBuckets listBucketsCommand = new ListBuckets.Builder(namespace.getBucketType()).build();

        final ListBuckets.Response listResponse = client.execute(listBucketsCommand);

        Iterator<Namespace> iterator = listResponse.iterator();
        assertTrue(iterator.hasNext());
        boolean found = false;

        while (!found && iterator.hasNext())
        {
            found = iterator.next().getBucketName().toString().equals(bucketName);
        }

        assertTrue(found);
    }

    private void testListBucketsStreaming(Namespace namespace) throws InterruptedException, ExecutionException
    {
        ListBuckets listBucketsCommand = new ListBuckets.Builder(namespace.getBucketType()).build();

        final RiakFuture<ListBuckets.StreamingResponse, BinaryValue> streamingFuture =
                client.executeAsyncStreaming(listBucketsCommand, 500);

        final ListBuckets.StreamingResponse streamResponse = streamingFuture.get();
        final Iterator<Namespace> iterator = streamResponse.iterator();

        assumeTrue(iterator.hasNext());
        boolean found = false;

        for (Namespace ns : streamResponse)
        {
            if(!found)
            {
                found = ns.getBucketName().toString().equals(bucketName);
            }
        }

        streamingFuture.await(); // Wait for command to finish, even if we've found our data
        assumeTrue(streamingFuture.isDone());

        assertFalse(iterator.hasNext());
        assertEquals(namespace.getBucketType(), streamingFuture.getQueryInfo());

        assertTrue(found);
    }

    private static void storeTestObject(Namespace namespace) throws ExecutionException, InterruptedException
    {
        // Empty buckets do not show up
        final BinaryValue key = BinaryValue.unsafeCreate("temp_key".getBytes());

        RiakObject value = new RiakObject().setValue(BinaryValue.create("{\"value\":\"value\"}"));

        // Since bucket type in response is populated from the command's context,
        // need a way to make sure the type is indeed as expected - use bucket type for bucket name
        Location location = new Location(namespace, key);
        StoreValue storeCommand = new StoreValue.Builder(value).withLocation(location).build();

        client.execute(storeCommand);
    }
}
