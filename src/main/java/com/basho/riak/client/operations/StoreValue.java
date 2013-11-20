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

import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.util.ByteArrayWrapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

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
        return null;
    }

    public static class Response<T>
    {

        private final boolean notFound;
        private final boolean unchanged;
        private final ByteArrayWrapper key;
        private final VClock vClock;
        private final List<T> value;

        Response(boolean notFound, boolean unchanged, List<T> value, VClock vClock, ByteArrayWrapper key)
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

        public ByteArrayWrapper getKey()
        {
            return key;
        }

    }
}
