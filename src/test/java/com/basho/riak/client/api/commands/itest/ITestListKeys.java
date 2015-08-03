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

package com.basho.riak.client.api.commands.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.buckets.ListBuckets;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakResultStreamListener;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Assume;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author Chris Mancini <cmancini at basho dot com>
 */
public class ITestListKeys extends ITestBase
{
    private final static RiakClient client = new RiakClient(cluster);

    public void generateKeys() throws ExecutionException, InterruptedException
    {
        Assume.assumeTrue(testBucketType);

        Namespace ns = new Namespace(bucketType.toString(), bucketName.toString());
        for (int i = 0; i < 200; i++)
        {
            Location loc = new Location(ns, "list_keys_test_" + i);
            RiakObject ro = new RiakObject().setContentType("text/plain")
                    .setValue(BinaryValue.create(Integer.toString(i)));
            StoreValue sv = new StoreValue.Builder(ro).withLocation(loc).build();
            RiakFuture<StoreValue.Response, Location> future = client.executeAsync(sv);
            future.await();
            assertTrue(future.isSuccess());
        }
    }

    @Test
    public void streamingTest() throws ExecutionException, InterruptedException
    {
        generateKeys();

        // Empty buckets do not show up
        final BinaryValue key = BinaryValue.unsafeCreate("my_key".getBytes());
        final String value = "{\"value\":\"value\"}";

        final LinkedBlockingQueue<Namespace> results = new LinkedBlockingQueue<Namespace>();

        RiakObject rObj = new RiakObject().setValue(BinaryValue.create(value));
        Location location = new Location(new Namespace(bucketType.toString(), bucketName.toString()), key);
        StoreValue sv =
                new StoreValue.Builder(rObj).withLocation(location)
                        .build();

        client.execute(sv);

        RiakResultStreamListener<ListBuckets.Response> streamListener =
                new RiakResultStreamListener<ListBuckets.Response>() {
                    @Override
                    public void handle(ListBuckets.Response response) {
                        for(Namespace bucket : response)
                        {
                            results.add(bucket);
                        }
                    }
                };

        ListBuckets listCom = new ListBuckets.Builder(bucketType.toString())
                .withResultStreamListener(streamListener)
                .build();

        RiakFuture<ListBuckets.Response, BinaryValue> future = client.executeAsync(listCom);

        future.await();
        assertTrue(future.isSuccess());

        ListBuckets.Response response = future.get();

        assertTrue(results.size() > 0);
        assertFalse(response.iterator().hasNext());
    }
}
