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
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.buckets.ListBuckets;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakResultStreamListener;
import com.basho.riak.client.core.operations.ListBucketsOperation;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author Chris Mancini <cmancini at basho dot com>
 */
public class ITestListBuckets extends ITestBase
{
    @Test
    public void streamingTest() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

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

        ListBuckets listCom = new ListBuckets.Builder(bucketType)
                .withResultStreamListener(streamListener)
                .build();

        RiakFuture<ListBuckets.Response, BinaryValue> future = client.executeAsync(listCom);

        future.await();
        // streaming complete, assert that the bucket was found
        assertFalse(future.get().iterator().hasNext());
        assertEquals(1, results.size());
    }
}
