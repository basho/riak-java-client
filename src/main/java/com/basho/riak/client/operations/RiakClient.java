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

import com.basho.riak.client.cap.*;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.convert.PassThroughConverter;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.operations.datatypes.DatatypeConverter;
import com.basho.riak.client.operations.datatypes.DatatypeMutation;
import com.basho.riak.client.operations.datatypes.RiakDatatype;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.ByteArrayWrapper;

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.basho.riak.client.operations.Location.bucket;

public class RiakClient
{

    private static final Converter<RiakObject> DEFAULT_CONVERTER = new PassThroughConverter();

    private final RiakCluster cluster;

    public RiakClient(RiakCluster cluster)
    {
        this.cluster = cluster;
    }

    public <T> FetchValue<T> fetch(Key location, Converter<T> converter)
    {
        return new FetchValue(cluster, location, converter);
    }

    public FetchValue<RiakObject> fetch(Key location)
    {
        return new FetchValue<RiakObject>(cluster, location, DEFAULT_CONVERTER);
    }

    public <T extends RiakDatatype> FetchDatatype<T> fetch(Key location, DatatypeConverter<T> converter)
    {
        return new FetchDatatype<T>();
    }

    public <T> StoreValue<T> store(Location location, T value, Converter<T> converter)
    {
        return new StoreValue<T>(cluster, location, value, converter);
    }

    public <T> StoreValue<T> store(Location location, T value, VClock vClock, Converter<T> converter)
    {
        StoreValue<T> sv = new StoreValue<T>(cluster, location, value, converter);
        sv.withVectorClock(vClock);
        return sv;
    }

    public StoreValue<RiakObject> store(Location location, RiakObject value)
    {
        return new StoreValue<RiakObject>(cluster, location, value, DEFAULT_CONVERTER);
    }

    public StoreValue<RiakObject> store(Location location, RiakObject value, VClock vClock)
    {
        StoreValue<RiakObject> sv = new StoreValue<RiakObject>(cluster, location, value, DEFAULT_CONVERTER);
        sv.withVectorClock(vClock);
        return sv;
    }

    public <T> UpdateValue<T> update(Key location, Converter<T> converter, ConflictResolver<T> resolver, Update<T> update)
    {
        return new UpdateValue<T>(cluster, location, converter, resolver, update);
    }

    public <T> UpdateValue<T> update(Key location, Converter<T> converter, Update<T> update)
    {
        return new UpdateValue<T>(cluster, location, converter, new DefaultResolver<T>(), update);
    }

    public UpdateValue<RiakObject> update(Key location, ConflictResolver<RiakObject> resolver, Update<RiakObject> update)
    {
        return new UpdateValue<RiakObject>(cluster, location, DEFAULT_CONVERTER, resolver, update);
    }

    public UpdateValue<RiakObject> update(Key location, Update<RiakObject> update)
    {
        return new UpdateValue<RiakObject>(cluster, location, DEFAULT_CONVERTER, new DefaultResolver<RiakObject>(), update);
    }

    public <T extends RiakDatatype> UpdateDatatype<T> update(Key location, T datatype)
    {
        return new UpdateDatatype<T>();
    }

    public <T extends RiakDatatype> UpdateDatatype<T> update(Key location, DatatypeMutation<T> mutation)
    {
        return new UpdateDatatype<T>();
    }

    public <T> UpdateValue<T> resolve(Key location, Converter<T> converter, ConflictResolver<T> resolver)
    {
        return new UpdateValue<T>(cluster, location, converter, resolver, Update.<T>identity());
    }

    public UpdateValue<RiakObject> resolve(Key location, ConflictResolver<RiakObject> resolver)
    {
        return new UpdateValue<RiakObject>(cluster, location, DEFAULT_CONVERTER, resolver, Update.<RiakObject>identity());
    }

    public DeleteValue delete(Key location)
    {
        return new DeleteValue(cluster, location);
    }

    public static RiakObject resolve(List<RiakObject> siblings)
    {
        return siblings.get(0);
    }

    public static RiakObject mutate(RiakObject o)
    {
        String original = o.getValue().toString();
        System.out.println(original);
        String updated = original +  "hi";
        o.setValue(ByteArrayWrapper.create(updated));
        return o;
    }

    public static void main(String[] args) throws UnknownHostException, ExecutionException, InterruptedException
    {

        // Create a Cluster
        RiakNode node = new RiakNode.Builder().withRemoteAddress("localhost").build();
        RiakCluster cluster = new RiakCluster.Builder(node).build();

        cluster.start();

        RiakClient client = new RiakClient(cluster);

        Bucket bucket = bucket("bucket-o-stuff");

        // Store something
        RiakObject obj = new RiakObject();
        obj.setValue(ByteArrayWrapper.create("stuff"));

        StoreValue.Response<RiakObject> initialStore = client.store(bucket, obj)
            .withOption(StoreOption.RETURN_BODY, true)
            .execute();
        System.out.println(initialStore.getValue().get(0).getValue());

        // (type, bucket, key) are represented as Locations
        Key key = initialStore.getKey();

        // A simple fetch
        FetchValue.Response<RiakObject> simple = client.fetch(key).execute();
        System.out.println(simple.getValue().get(0).getValue());

        // A fetch using FetchOptions
        FetchValue.Response<RiakObject> fetch = client.fetch(key)
            .withOption(FetchOption.TIMEOUT, 1000)
            .execute();
        System.out.println(fetch.getValue().get(0).getValue());

        // Manual resolution
        obj = resolve(fetch.getValue());

        // Store the resolved object back
        client.store(key, obj, fetch.getvClock())
//            .withOption(StoreOption.RETURN_HEAD, true) // TODO RETURN_HEAD is broken
            .withOption(StoreOption.TIMEOUT, 1000)
            .execute();

        // Store a bunch of things in a bucket with riak assigning keys
        for (int i = 0; i < 1000; ++i)
        {
            StoreValue.Response<RiakObject> response =
                client.store(bucket, obj).execute();
            System.out.println(response.getKey());
        }

        // Represent anything that has to fetch, then resolve, then store back
        // as an update
        UpdateValue.Response<RiakObject> update =
            client.update(key, new Update<RiakObject>()
            {
                @Override
                public RiakObject apply(RiakObject original)
                {
                    return mutate(original);
                }

            }).withStoreOption(StoreOption.RETURN_BODY, true).execute();

        System.out.println(update.getValue().get(0).getValue());

        // Resolve conflicts for a given key
        client.resolve(key, Resolvers.MyResolver);

        // Delete a value
        client.delete(key).execute();
//
//        // Fetch a Counter CRDT
//        Key counterKey = key("counters", "counter");
//        FetchDatatype.Response<RiakCounter> counterResponse =
//            client.fetch(counterKey, asCounter()).execute();
//
//        RiakCounter counter = counterResponse.getDatatype();
//        counter.increment(10000);
//        System.out.println(counter.view());
//
//        UpdateDatatype.Response<RiakCounter> updateResponse =
//            client.update(counterKey, counter).execute();
//
//        // OR, just fling an update at a CRDT...
//        client.update(counterKey, incrementBy(10000)).execute();
//
//        ByteArrayWrapper name = ByteArrayWrapper.create("element");
//        client.update(key("sets", "set"), addElement(name)).execute();
//
//        // Create a new datatype and store it
//        RiakMap myMap = new RiakMap();
//        myMap.put("counter", new RiakCounter());
//        client.update(key("maps", "my_map"), myMap);

        cluster.shutdown();

    }

    static enum Resolvers implements ConflictResolver<RiakObject>
    {
        MyResolver;

        @Override
        public RiakObject resolve(List<RiakObject> objectList) throws UnresolvedConflictException
        {
            return null;
        }

    }

}
