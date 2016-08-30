/*
 * Copyright 2013 Basho Technologies Inc
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
package com.basho.riak.client.integ;

import com.basho.riak.client.FetchMeta;
import com.basho.riak.client.StoreMeta;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.convert.PassThroughConverter;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.DeleteOperation;
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.BinaryValue;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

public class StoreFetchDeleteTest
{
    private final Converter<RiakObject> domainObjectConverter = new PassThroughConverter();
    private static RiakCluster cluster;
    private BinaryValue bucket = BinaryValue.create("bucket");

    @BeforeClass
    public static void setup() throws UnknownHostException
    {
        RiakNode node = new RiakNode.Builder()
            .withRemoteAddress("localhost")
            .withRemotePort(8087)
            .build();

        cluster = RiakCluster.builder(node).build();
        cluster.start();
    }

    @AfterClass
    public static void teardown()
    {
        cluster.stop();
    }

    @Test
    public void testStoreFetchDelete() throws ExecutionException, InterruptedException
    {
        RiakObject o = RiakObject.create(bucket.unsafeGetValue()).setValue("test value");
        StoreMeta storeMeta = new StoreMeta.Builder().returnBody(true).build();

        StoreOperation<RiakObject> store =
            new StoreOperation<RiakObject>(bucket, o)
                .withConverter(domainObjectConverter)
                .withStoreMeta(storeMeta);

        cluster.execute(store);

        RiakObject storeReturn = store.get();

        BinaryValue returnedKey = BinaryValue.create(storeReturn.getBucketAsBytes());
        FetchOperation<RiakObject> fetch =
            new FetchOperation<RiakObject>(bucket, returnedKey)
            .withConverter(domainObjectConverter);

        cluster.execute(fetch);

        RiakObject fetchReturn = fetch.get();

        DeleteOperation delete = new DeleteOperation(bucket, returnedKey);

        cluster.execute(delete);

        delete.get();

        FetchOperation<RiakObject> tombstoneFetch =
            new FetchOperation<RiakObject>(bucket, returnedKey)
            .withConverter(domainObjectConverter);

        cluster.execute(tombstoneFetch);

        RiakObject tombstone = tombstoneFetch.get();

        Assert.assertTrue(tombstone.isNotFound());
    }

    @Test
    public void testSiblings() throws ExecutionException, InterruptedException
    {
        RiakObject o = RiakObject.create(bucket.unsafeGetValue()).setValue("test value");
        StoreMeta storeMeta = new StoreMeta.Builder().returnBody(true).build();

        StoreOperation<RiakObject> store1 =
            new StoreOperation<RiakObject>(bucket, o)
                .withConverter(domainObjectConverter)
                .withStoreMeta(storeMeta);

        cluster.execute(store1);

        RiakObject storeReturn1 = store1.get();

        BinaryValue key = BinaryValue.create(storeReturn1.getKeyAsBytes());
        StoreOperation<RiakObject> store2 =
            new StoreOperation<RiakObject>(bucket, key, o)
                .withConverter(domainObjectConverter)
                .withStoreMeta(storeMeta);

        cluster.execute(store2);

        RiakObject storeReturn2 = store2.get();
    }
}
