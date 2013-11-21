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
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.core.RiakCluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class UpdateValue<T> implements RiakCommand<UpdateValue.Response<T>>
{

    private final RiakCluster cluster;
    private final Location location;
    private final Converter<T> converter;
    private final Update<T> update;
    private final ConflictResolver<T> resolver;
    private final Map<FetchOption, Object> fetchOptions;
    private final Map<StoreOption, Object> storeOptions;

    UpdateValue(RiakCluster cluster, Location location, Converter<T> converter, ConflictResolver<T> resolver, Update<T> update)
    {
        this.cluster = cluster;
        this.location = location;
        this.converter = converter;
        this.resolver = resolver;
        this.update = update;
        this.fetchOptions = new HashMap<FetchOption, Object>();
        this.storeOptions = new HashMap<StoreOption, Object>();

    }

    public <U> UpdateValue<T> withFetchOption(FetchOption<U> option, U value)
    {
        fetchOptions.put(option, value);
        return this;
    }

    public <U> UpdateValue<T> withStoreOption(StoreOption<U> option, U value)
    {
        storeOptions.put(option, value);
        return this;
    }

    @Override
    public Response<T> execute() throws ExecutionException, InterruptedException
    {

        FetchValue<T> fetch = new FetchValue<T>(cluster, location, converter);
        for (Map.Entry<FetchOption, Object> optPair : fetchOptions.entrySet())
        {
            fetch.withOption(optPair.getKey(), optPair.getValue());
        }

        FetchValue.Response<T> fetchResponse = fetch.execute();

        List<T> value = fetchResponse.getValue();
        T resolved = resolver.resolve(value);
        T updated = update.apply(resolved);

        if (update.isModified())
        {

            StoreValue<T> store = new StoreValue<T>(cluster, location, updated, converter);
            for (Map.Entry<StoreOption, Object> optPair : storeOptions.entrySet())
            {
                store.withOption(optPair.getKey(), optPair.getValue());
            }
            StoreValue.Response<T> storeResponse = store.execute();

            List<T> values = storeResponse.getValue();
            VClock clock = storeResponse.getvClock();

            return new Response<T>(values, clock);

        }

        return new Response<T>(value, fetchResponse.getvClock());
    }

    public static class Response<T>
    {

        private final VClock vClock;
        private final List<T> value;

        Response(List<T> value, VClock vClock)
        {
            this.value = value;
            this.vClock = vClock;
        }

        public VClock getvClock()
        {
            return vClock;
        }

        public List<T> getValue()
        {
            return value;
        }

    }
}
