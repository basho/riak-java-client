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

import com.basho.riak.client.api.ListException;
import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.ListKeys;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 *
 * @author Alex Moore <amoore at basho dot com>
 */
public class ITestListKeys extends ITestBase
{
    private static final RiakClient client = new RiakClient(cluster);
    private static final String bucketName = "ITestListBuckets" + new Random().nextLong();
    private static final Namespace typedNamespace = new Namespace(bucketType.toString(), bucketName);

    @BeforeClass
    public static void setup() throws ExecutionException, InterruptedException
    {
        if (testBucketType)
        {
            storeTestObjects(typedNamespace);
        }
    }

    @AfterClass
    public static void cleanup() throws ExecutionException, InterruptedException
    {
        if (testBucketType)
        {
            resetAndEmptyBucket(typedNamespace);
        }
    }

    @Test
    public void testLargeStreamingListKeys() throws ListException, ExecutionException, InterruptedException
    {
        assumeTrue(testBucketType);

        ListKeys lk = new ListKeys.Builder(typedNamespace).withAllowListing().build();

        final RiakFuture<ListKeys.Response, Namespace> streamFuture =
                client.executeAsyncStreaming(lk, 200);

        final ListKeys.Response streamingResponse = streamFuture.get();

        int count = 0;
        boolean foundLastKey = false;

        for (Location location : streamingResponse)
        {
            count++;

            if (!foundLastKey)
            {
                foundLastKey = location.getKeyAsString().equals("9999");
            }
        }

        streamFuture.await();
        assertTrue(foundLastKey);
        assertTrue(streamFuture.isDone());
        assertEquals(10000, count);
    }

    private static void storeTestObjects(Namespace namespace) throws InterruptedException
    {
        final String value = "{\"value\":\"value\"}";
        final RiakObject rObj = new RiakObject().setValue(BinaryValue.create(value));

        for (int i = 0; i < 10000; i++)
        {
            final String key = Integer.toString(i);

            final Location location = new Location(namespace, key);
            final StoreOperation storeOp =
                    new StoreOperation.Builder(location)
                            .withContent(rObj)
                            .build();

            final RiakFuture<StoreOperation.Response, Location> execute = cluster.execute(storeOp);
            execute.await();
            assertTrue(execute.isSuccess());
        }
    }
}
