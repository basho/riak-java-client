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
import com.basho.riak.client.cap.Mutation;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.core.RiakCluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MutateValue<T> implements RiakCommand<T>
{

    private final RiakCluster cluster;
    private final Location location;
    private final Converter<T> converter;
    private final Mutation<T> mutation;
    private final ConflictResolver<T> resolver;
    private final Map<FetchOption, Object> fetchOptions;
    private final Map<StoreOption, Object> storeOptions;

    MutateValue(RiakCluster cluster, Location location, Converter<T> converter, ConflictResolver<T> resolver, Mutation<T> mutation)
    {
        this.cluster = cluster;
        this.location = location;
        this.converter = converter;
        this.resolver = resolver;
        this.mutation = mutation;
        this.fetchOptions = new HashMap<FetchOption, Object>();
        this.storeOptions = new HashMap<StoreOption, Object>();

    }

    public <T> MutateValue withFetchOption(FetchOption<T> option, T value)
    {
        fetchOptions.put(option, value);
        return this;
    }

    public <T> MutateValue withStoreOption(StoreOption<T> option, T value)
    {
        storeOptions.put(option, value);
        return this;
    }

    @Override
    public T execute()
    {
        return null;
    }
}
