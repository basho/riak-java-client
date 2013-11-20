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
import com.basho.riak.client.convert.Converters;
import com.basho.riak.client.convert.PassThroughConverter;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.query.RiakObject;

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.basho.riak.client.cap.Quorum.allQuorum;
import static com.basho.riak.client.operations.RiakClient.Resolvers.MyResolver;

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

    public <T> StoreValue<T> store(Location location, T value, VClock vClock)
    {
        StoreValue<T> sv = new StoreValue<T>(cluster, location, value, Converters.<T>jsonConverter());
        sv.withVectorClock(vClock);
        return sv;
    }

    public StoreValue<RiakObject> store(Location location, RiakObject value)
    {
        return new StoreValue<RiakObject>(cluster, location, value, DEFAULT_CONVERTER);
    }

    public <T> StoreValue<T> store(Location location, T value)
    {
        return new StoreValue<T>(cluster, location, value, Converters.<T>jsonConverter());
    }

    public StoreValue<RiakObject> store(Location location, RiakObject value, VClock vClock)
    {
        StoreValue<RiakObject> sv = new StoreValue<RiakObject>(cluster, location, value, DEFAULT_CONVERTER);
        sv.withVectorClock(vClock);
        return sv;
    }

    public <T> MutateValue<T> update(Key location, Converter<T> converter, ConflictResolver<T> resolver, Mutation<T> mutation)
    {
        return new MutateValue<T>(cluster, location, converter, resolver, mutation);
    }

    public <T> MutateValue<T> update(Key location, Converter<T> converter, Mutation<T> mutation)
    {
        return new MutateValue<T>(cluster, location, converter, new DefaultResolver<T>(), mutation);
    }

    public MutateValue<RiakObject> update(Key location, ConflictResolver<RiakObject> resolver, Mutation<RiakObject> mutation)
    {
        return new MutateValue<RiakObject>(cluster, location, DEFAULT_CONVERTER, resolver, mutation);
    }

    public MutateValue<RiakObject> update(Key location, Mutation<RiakObject> mutation)
    {
        return new MutateValue<RiakObject>(cluster, location, DEFAULT_CONVERTER, new DefaultResolver<RiakObject>(), mutation);
    }

    public <T> MutateValue<T> resolve(Key location, Converter<T> converter, ConflictResolver<T> resolver)
    {
        return new MutateValue<T>(cluster, location, converter, resolver, Mutations.<T>identity());
    }

    public MutateValue<RiakObject> resolve(Key location, ConflictResolver<RiakObject> resolver)
    {
        return new MutateValue<RiakObject>(cluster, location, DEFAULT_CONVERTER, resolver, Mutations.<RiakObject>identity());
    }

    public DeleteValue delete(Key location)
    {
        return new DeleteValue(cluster, location);
    }

    public static RiakObject resolve(List<RiakObject> siblings)
    {
        return null;
    }

    public static RiakObject mutate(RiakObject o)
    {
        return null;
    }

    public static void main(String[] args) throws UnknownHostException, ExecutionException, InterruptedException
    {

        // Create a Cluster
        RiakNode node = new RiakNode.Builder().build();
        RiakCluster cluster = new RiakCluster.Builder(node).build();
        RiakClient client = new RiakClient(cluster);

        // (type, bucket, key) are represented as Locations
        Key key = Location.key("bucket", "key");

        // A simple fetch
        FetchValue.Response<RiakObject> simple = client.fetch(key).execute();
        System.out.println(simple.getValue());

        // A more complex fetch using FetchOptions
        FetchValue.Response<RiakObject> fetch = client.fetch(key)
            .withOption(FetchOption.BASIC_QUORUM, true)
            .withOption(FetchOption.DELETED_VCLOCK, true)
            .withOption(FetchOption.TIMEOUT, 1000)
            .withOption(FetchOption.R, allQuorum())
            .execute();

        // Manual resolution
        RiakObject obj = resolve(fetch.getValue());

        // Store the resolved object back
        client.store(key, obj, fetch.getvClock())
            .withOption(StoreOption.RETURN_HEAD, true)
            .withOption(StoreOption.TIMEOUT, 1000)
            .execute();

        // Store a bunch of things in a bucket with riak assigning keys
        Bucket bucket = Location.bucket("dave");
        for (int i = 0; i < 100; ++i)
        {
            StoreValue.Response<RiakObject> response =
                client.store(bucket, obj).execute();
            System.out.println(response.getKey());
        }

        // Represent anything that has to fetch, then resolve, then store back
        // as an update
        Key key2 = Location.key("bucket", "key");
        client.update(key2, new BaseMutation<RiakObject>()
        {
            @Override
            public RiakObject apply(RiakObject original)
            {
                RiakObject mutated = mutate(original);
                setHasMutated(mutated != null);
                return mutated;
            }

        }).execute();

        // Resolve conflicts for a given key
        client.resolve(key, MyResolver);

        // Delete a value
        DeleteValue.Response delete = client.delete(key).execute();


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
