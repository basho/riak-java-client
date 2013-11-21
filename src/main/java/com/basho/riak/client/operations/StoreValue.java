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

import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.convert.Converters;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.query.KvResponse;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.ByteArrayWrapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.basho.riak.client.convert.Converters.convert;

public class StoreValue<V> implements RiakCommand<StoreValue.Response<V>>
{

    private final RiakCluster cluster;
    private final Location location;
    private final Map<StoreOption<?>, Object> options;
    private final V value;
    private final Converter<V> converter;
    private VClock vClock;

    StoreValue(RiakCluster cluster, Location location, V value, Converter<V> converter)
    {
        this.cluster = cluster;
        this.options = new HashMap<StoreOption<?>, Object>();
        this.location = location;
        this.value = value;
        this.converter = converter;
    }

    public StoreValue<V> withVectorClock(VClock vClock)
    {
        this.vClock = vClock;
        return this;
    }

    public <T> StoreValue<V> withOption(StoreOption<T> option, T value)
    {
        options.put(option, value);
        return this;
    }

    public Location getLocation()
    {
        return location;
    }

    public V getValue()
    {
        return value;
    }

    @Override
    public Response<V> execute() throws ExecutionException, InterruptedException
    {

        ByteArrayWrapper type = location.getType();
        ByteArrayWrapper bucket = location.getBucket();
        ByteArrayWrapper key = location.getKey();

        StoreOperation.Builder builder = new StoreOperation.Builder(bucket);

        if (type != null)
        {
            builder.withBucketType(type);
        }

        if (key != null)
        {
            builder.withKey(key);
        }

        builder.withContent(converter.fromDomain(value));

        if (vClock != null)
        {
            builder.withVClock(vClock);
        }

        for (Map.Entry<StoreOption<?>, Object> opPair : options.entrySet())
        {

            RiakOption<?> option = opPair.getKey();

            if (option == StoreOption.TIMEOUT)
            {
                builder.withTimeout((Integer) opPair.getValue());
            }
            else if (option == StoreOption.RETURN_HEAD)
            {
                builder.withReturnHead((Boolean) opPair.getValue());
            }
            else if (option == StoreOption.ASIS)
            {
                builder.withAsis((Boolean) opPair.getValue());
            }
            else if (option == StoreOption.DW)
            {
                builder.withDw(((Quorum) opPair.getValue()).getIntValue());
            }
            else if (option == StoreOption.IF_NONE_MATCH)
            {
                builder.withIfNoneMatch((Boolean) opPair.getValue());
            }
            else if (option == StoreOption.IF_NOT_MODIFIED)
            {
                builder.withIfNotModified((Boolean) opPair.getValue());
            }
            else if (option == StoreOption.N_VAL)
            {
                builder.withNVal((Integer) opPair.getValue());
            }
            else if (option == StoreOption.PW)
            {
                builder.withPw(((Quorum) opPair.getValue()).getIntValue());
            }
            else if (option == StoreOption.SLOPPY_QUORUM)
            {
                builder.withSloppyQuorum((Boolean) opPair.getValue());
            }
            else if (option == StoreOption.W)
            {
                builder.withW(((Quorum) opPair.getValue()).getIntValue());
            }
            else if (option == StoreOption.RETURN_BODY)
            {
                builder.withReturnBody((Boolean) opPair.getValue());
            }

        }

        StoreOperation operation = builder.build();
        cluster.execute(operation);
                                  
        KvResponse<List<RiakObject>> response = operation.get();
        List<V> converted = convert(converter, response.getContent());

        Key k = Location.key(type, bucket, response.getKey());
        VClock clock = response.getVClock();
        boolean unchanged = response.unchanged();
        boolean notFound = response.notFound();

        return new Response<V>(notFound, unchanged, converted, clock, k);

    }

    public static class Response<T>
    {

        private final boolean notFound;
        private final boolean unchanged;
        private final Key key;
        private final VClock vClock;
        private final List<T> value;

        Response(boolean notFound, boolean unchanged, List<T> value, VClock vClock, Key key)
        {
            this.notFound = notFound;
            this.unchanged = unchanged;
            this.value = value;
            this.vClock = vClock;
            this.key = key;
        }

        public boolean isNotFound()
        {
            return notFound;
        }

        public boolean isUnchanged()
        {
            return unchanged;
        }

        public boolean hasvClock()
        {
            return vClock != null;
        }

        public VClock getvClock()
        {
            return vClock;
        }

        public boolean hasValue()
        {
            return value != null;
        }

        public List<T> getValue()
        {
            return value;
        }

        public Key getKey()
        {
            return key;
        }

    }
}
