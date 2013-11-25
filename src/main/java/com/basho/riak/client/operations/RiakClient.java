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
import com.basho.riak.client.cap.DefaultResolver;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.convert.PassThroughConverter;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.ListBucketsOperation;
import com.basho.riak.client.core.operations.ListKeysOperation;
import com.basho.riak.client.operations.datatypes.DatatypeConverter;
import com.basho.riak.client.operations.datatypes.DatatypeMutation;
import com.basho.riak.client.operations.datatypes.RiakDatatype;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.ByteArrayWrapper;

import java.net.UnknownHostException;
import java.util.Iterator;
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

    public RiakCommand<Iterable<Key>> listKeys(final Bucket bucket, final int timeout)
    {
        return new RiakCommand<Iterable<Key>>()
        {
            @Override
            public Iterable<Key> execute() throws ExecutionException, InterruptedException
            {
                ByteArrayWrapper b = bucket.getBucket();
                ListKeysOperation operation = timeout > 0
                    ? new ListKeysOperation(b, timeout)
                    : new ListKeysOperation(b);
                operation.withBucketType(bucket.getType());
                cluster.execute(operation);
                return new KeyIterable(operation.get(), bucket);
            }
        };
    }

    public RiakCommand<Iterable<Key>> listKeys(Bucket bucket)
    {
        return listKeys(bucket, -1);
    }

    public RiakCommand<Iterable<Bucket>> listBuckets(final BucketType type, final int timeout, final boolean stream)
    {
        return new RiakCommand<Iterable<Bucket>>()
        {
            @Override
            public Iterable<Bucket> execute() throws ExecutionException, InterruptedException
            {
                ListBucketsOperation operation = timeout > 0
                    ? new ListBucketsOperation(timeout, stream)
                    : new ListBucketsOperation();
                operation.withBucketType(type.getType());
                cluster.execute(operation);
                return new BucketIterable(operation.get(), type);
            }
        };
    }

    public RiakCommand<Iterable<Bucket>> listBuckets(BucketType type)
    {
        return listBuckets(type, -1, false);
    }

    public static RiakObject resolve(List<RiakObject> siblings)
    {
        return siblings.get(0);
    }

    public static RiakObject mutate(RiakObject o)
    {
        String original = o.getValue().toString();
        System.out.println(original);
        String updated = original + "hi";
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

        Iterable<Key> keys = client.listKeys(bucket).execute();
        for (Key k : keys)
        {
            System.out.println(k);
        }

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

    private static class KeyIterable implements Iterable<Key>
    {

        private final Iterable<ByteArrayWrapper> iterable;
        private final Bucket bucket;

        private KeyIterable(Iterable<ByteArrayWrapper> iterable, Bucket bucket)
        {
            this.iterable = iterable;
            this.bucket = bucket;
        }

        @Override
        public Iterator<Key> iterator()
        {
            return new Itr(iterable.iterator(), bucket);
        }

        private static class Itr implements Iterator<Key>
        {
            private final Iterator<ByteArrayWrapper> iterator;
            private final Bucket bucket;

            private Itr(Iterator<ByteArrayWrapper> iterator, Bucket bucket)
            {
                this.iterator = iterator;
                this.bucket = bucket;
            }

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public Key next()
            {
                ByteArrayWrapper key = iterator.next();
                return Location.key(bucket, key);
            }

            @Override
            public void remove()
            {
                iterator.remove();
            }
        }
    }

    private static class BucketIterable implements Iterable<Bucket>
    {

        private final Iterable<ByteArrayWrapper> iterable;
        private final BucketType type;

        private BucketIterable(Iterable<ByteArrayWrapper> iterable, BucketType type)
        {
            this.iterable = iterable;
            this.type = type;
        }

        @Override
        public Iterator<Bucket> iterator()
        {
            return new Itr(iterable.iterator(), type);
        }

        private static class Itr implements Iterator<Bucket>
        {
            private final Iterator<ByteArrayWrapper> iterator;
            private final BucketType type;

            private Itr(Iterator<ByteArrayWrapper> iterator, BucketType type)
            {
                this.iterator = iterator;
                this.type = type;
            }

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public Bucket next()
            {
                ByteArrayWrapper bucket = iterator.next();
                return Location.bucket(type, bucket);
            }

            @Override
            public void remove()
            {
                iterator.remove();
            }
        }
    }

}
