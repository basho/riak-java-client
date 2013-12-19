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
package com.basho.riak.client.operations;

import com.basho.riak.client.cap.ConflictResolver;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.convert.PassThroughConverter;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.operations.datatypes.CounterUpdate;
import com.basho.riak.client.operations.datatypes.MapUpdate;
import com.basho.riak.client.operations.datatypes.RiakCounter;
import com.basho.riak.client.operations.datatypes.SetUpdate;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.ByteArrayWrapper;

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static com.basho.riak.client.operations.FetchIndex.match;
import static com.basho.riak.client.operations.FetchIndex.range;
import static com.basho.riak.client.operations.FetchValue.fetch;
import static com.basho.riak.client.operations.Index.binIndex;
import static com.basho.riak.client.operations.Index.intIndex;
import static com.basho.riak.client.operations.Location.bucket;
import static com.basho.riak.client.operations.MultiFetch.multiFetch;
import static com.basho.riak.client.operations.StoreValue.store;
import static com.basho.riak.client.operations.UpdateValue.resolve;

public class RiakClient
{

    private static final Converter<RiakObject> DEFAULT_CONVERTER = new PassThroughConverter();

    private final RiakCluster cluster;
    private final Executor executor;

    public RiakClient(RiakCluster cluster, Executor executor)
    {
        this.cluster = cluster;
        this.executor = executor;
    }

    public <T> T execute(RiakCommand<T> command) throws ExecutionException, InterruptedException
    {
        return command.execute(cluster);
    }

    public static void main(String[] args) throws UnknownHostException, ExecutionException, InterruptedException
    {

        // Create a Cluster
        RiakNode node = new RiakNode.Builder().withRemoteAddress("localhost").build();
        RiakCluster cluster = new RiakCluster.Builder(node).build();

        cluster.start();

        RiakClient client = new RiakClient(cluster, Executors.newCachedThreadPool());

        Bucket bucket = bucket("bucket-o-stuff");

        // Store something
        RiakObject obj = new RiakObject();
        obj.setValue(ByteArrayWrapper.create("stuff"));

        StoreValue.Response<RiakObject> initialStore = client.execute(
            store(bucket, obj).withOption(StoreOption.RETURN_BODY, true));
        System.out.println(initialStore.getValue().get(0).getValue());

        // (type, bucket, key) are represented as Locations
        Key key = initialStore.getKey();

        // A simple fetch
        FetchValue.Response<RiakObject> simple = client.execute(fetch(key));
        System.out.println(simple.getValue().get(0).getValue());

        // A fetch using FetchOptions
        FetchValue.Response<RiakObject> fetch = client.execute(fetch(key)
            .withOption(FetchOption.TIMEOUT, 1000));
        System.out.println(fetch.getValue().get(0).getValue());

        // Manual resolution
        obj = fetch.getValue().get(0);

        // Store the resolved object back
        client.execute(store(key, obj, fetch.getvClock())
//            .withOption(StoreOption.RETURN_HEAD, true) // TODO RETURN_HEAD is broken
            .withOption(StoreOption.TIMEOUT, 1000));

        // Store a bunch of things in a bucket with riak assigning keys
        for (int i = 0; i < 1000; ++i)
        {
            StoreValue.Response<RiakObject> response =
                client.execute(store(bucket, obj));
            System.out.println(response.getKey());
        }

        // Represent anything that has to fetch, then resolve, then store back
        // as an update
        UpdateValue.Response<RiakObject> update =
            client.execute(UpdateValue.update(key, new UpdateValue.Update<RiakObject>()
            {
                @Override
                public RiakObject apply(RiakObject o)
                {
                    String original = o.getValue().toString();
                    System.out.println(original);
                    String updated = original + "hi";
                    o.setValue(ByteArrayWrapper.create(updated));
                    return o;
                }

            }).withStoreOption(StoreOption.RETURN_BODY, true));

        System.out.println(update.getValue().get(0).getValue());

        // Resolve conflicts for a given key
        client.execute(resolve(key, Resolvers.MyResolver));

        // Delete a value
        client.execute(new DeleteValue(key));

        Iterable<Key> keys = client.execute(new ListKeys(bucket));
        for (Key k : keys)
        {
            client.execute(new DeleteValue(k));
        }

        ListBuckets.Response buckets = client.execute(new ListBuckets());
        for (Bucket bucket1 : buckets)
        {
            System.out.println(bucket1);
        }

        FetchIndex.Response<Integer> r1 = client.execute(new FetchIndex<Integer>(bucket, intIndex("dave"), range(0, 100)));

        FetchIndex.Response<Integer> r2 = client.execute(new FetchIndex<Integer>(bucket, intIndex("dave"), match(12345)));

        FetchIndex.Response<byte[]> r3 = client.execute(new FetchIndex<byte[]>(bucket, binIndex("other"), match("12345")));

        FetchIndex.Response<byte[]> r4 = client.execute(new FetchIndex<byte[]>(bucket, binIndex("other"), match(new byte[]{0x1, 0x2, 0x3})));

        // Fetch a Counter CRDT
        Key counterKey = Location.key("counters", "counter");
        FetchDatatype.Response<RiakCounter> counterResponse =
            client.execute(FetchDatatype.fetchCounter(counterKey));

        RiakCounter counter = counterResponse.getDatatype();
        System.out.println(counter.view());

        UpdateDatatype.Response<RiakCounter> updateResponse =
            client.execute(UpdateDatatype
                .update(counterKey, new CounterUpdate(1000))
                .withOption(DtUpdateOption.RETURN_BODY, true));

        // OR, just fling an update at a CRDT...
        client.execute(UpdateDatatype.update(counterKey, new CounterUpdate(1000)));

        Key setKey = Location.key("sets", "myset");
        client.execute(UpdateDatatype.update(setKey, new SetUpdate().add(new byte[]{'0'})));

        Bucket mapBucket = Bucket.bucket("maps", "mymap");
        client.execute(UpdateDatatype.update(mapBucket, new MapUpdate()
            .add("things", new CounterUpdate(1000))
            .add("stuff", new SetUpdate().add(new byte[]{'1'}))
            .add("dodads", new MapUpdate()
                .addCounter("countedDodads"))));

        cluster.shutdown();

    }

    static enum Resolvers implements ConflictResolver<RiakObject>
    {
        MyResolver;

        @Override
        public RiakObject resolve(List<RiakObject> objectList) throws UnresolvedConflictException
        {
            return objectList.get(0);
        }

    }

}
