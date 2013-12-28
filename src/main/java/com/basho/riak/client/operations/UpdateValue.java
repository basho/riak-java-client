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
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.convert.PassThroughConverter;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.query.RiakObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Perform an full cycle update of a Riak value: fetch, resolve, modify, store.
 *
 * @param <T> the datatype that is being operated on
 */
public class UpdateValue<T> extends RiakCommand<UpdateValue.Response<T>>
{

    private final Location location;
    private final Converter<T> converter;
    private final Update<T> update;
    private final ConflictResolver<T> resolver;
    private final Map<FetchOption, Object> fetchOptions;
    private final Map<StoreOption, Object> storeOptions;

    UpdateValue(Location location, Converter<T> converter, ConflictResolver<T> resolver, Update<T> update)
    {
        this.location = location;
        this.converter = converter;
        this.resolver = resolver;
        this.update = update;
        this.fetchOptions = new HashMap<FetchOption, Object>();
        this.storeOptions = new HashMap<StoreOption, Object>();

    }

    /**
     * Create an update command to update a domain object in Riak
     *
     * @param location  where the object is located
     * @param converter converter to/from domain object
     * @param resolver  resolution strategy
     * @param update    business logic mutation
     * @param <T>       the domain object type
     * @return a response
     */
    public static <T> UpdateValue<T> update(Key location, Converter<T> converter, ConflictResolver<T> resolver, Update<T> update)
    {
        return new UpdateValue<T>(location, converter, resolver, update);
    }

    /**
     * Create an update command to update a domain object in Riak
     *
     * @param location  where the object is located
     * @param converter converter to/from domain object
     * @param update    business logic mutation
     * @param <T>       the domain object type
     * @return a response
     */
    public static <T> UpdateValue<T> update(Key location, Converter<T> converter, Update<T> update)
    {
        return new UpdateValue<T>(location, converter, new DefaultResolver<T>(), update);
    }

    /**
     * Create an update command to update a {@link RiakObject} in Riak
     *
     * @param location where the object is located
     * @param resolver resolution strategy
     * @param update   business logic mutation
     * @return a response
     */
    public static UpdateValue<RiakObject> update(Key location, ConflictResolver<RiakObject> resolver, Update<RiakObject> update)
    {
        return new UpdateValue<RiakObject>(location, new PassThroughConverter(), resolver, update);
    }

    /**
     * Create an update command to update a {@link RiakObject} in Riak
     *
     * @param location where the object is located
     * @param update   business logic mutation
     * @return a response
     */
    public static UpdateValue<RiakObject> update(Key location, Update<RiakObject> update)
    {
        return new UpdateValue<RiakObject>(location, new PassThroughConverter(), new DefaultResolver<RiakObject>(), update);
    }

    /**
     * Resolve siblings on a domain object using the given resolution strategy and conflict resolver.
     *
     * @param location  where the object is located
     * @param converter converter to/from domain object
     * @param resolver  resolution strategy
     * @param <T>       the domain object type
     * @return a response
     */
    public static <T> UpdateValue<T> resolve(Key location, Converter<T> converter, ConflictResolver<T> resolver)
    {
        return new UpdateValue<T>(location, converter, resolver, Update.<T>noopUpdate());
    }

    /**
     * Resolve siblings on a {@link RiakObject} using the given resolution strategy and conflict resolver.
     *
     * @param location where the object is located
     * @param resolver resolution strategy
     * @return a response
     */
    public static UpdateValue<RiakObject> resolve(Key location, ConflictResolver<RiakObject> resolver)
    {
        return new UpdateValue<RiakObject>(location, new PassThroughConverter(), resolver, Update.<RiakObject>noopUpdate());
    }

    /**
     * Add an option for the fetch phase of the update
     *
     * @param option the option
     * @param value  the option's value
     * @param <U>    the type of the option's value
     * @return this
     */
    public <U> UpdateValue<T> withFetchOption(FetchOption<U> option, U value)
    {
        fetchOptions.put(option, value);
        return this;
    }

    /**
     * Add an option for the store phase of the update
     *
     * @param option the option
     * @param value  the option's value
     * @param <U>    the type of the option's value
     * @return this
     */
    public <U> UpdateValue<T> withStoreOption(StoreOption<U> option, U value)
    {
        storeOptions.put(option, value);
        return this;
    }

    @Override
    public Response<T> execute(RiakCluster cluster) throws ExecutionException, InterruptedException
    {

        FetchValue<T> fetch = new FetchValue<T>(location).withConverter(converter);
        for (Map.Entry<FetchOption, Object> optPair : fetchOptions.entrySet())
        {
            fetch.withOption(optPair.getKey(), optPair.getValue());
        }

        FetchValue.Response<T> fetchResponse = fetch.execute(cluster);

        List<T> value = fetchResponse.getValue();
        T resolved = resolver.resolve(value);
        T updated = update.apply(resolved);

        if (update.isModified())
        {

            StoreValue<T> store = new StoreValue<T>(location, updated, converter);
            for (Map.Entry<StoreOption, Object> optPair : storeOptions.entrySet())
            {
                store.withOption(optPair.getKey(), optPair.getValue());
            }
            StoreValue.Response<T> storeResponse = store.execute(cluster);

            List<T> values = storeResponse.getValue();
            VClock clock = storeResponse.getvClock();

            return new Response<T>(values, clock);

        }

        return new Response<T>(value, fetchResponse.getvClock());
    }

    /**
     *
     * @param <T>
     */
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

    /**
     * An update on a Riak object
     *
     * @param <T>
     */
    public abstract static class Update<T>
    {

        private boolean modified = true;

        /**
         * Modify the input value and return the modification. It is OK to
         * modify the input value in-place and return it.
         *
         * @param original the resolved value
         * @return a modified value
         */
        public abstract T apply(T original);

        /**
         * Set the modification status of this update, defaults to {@code true}
         *
         * @param modified true if modified
         */
        protected void setModified(boolean modified)
        {
            this.modified = modified;
        }

        /**
         * true if this Update has modified the input value and requires a store,
         * defaults to {@code true}
         *
         * @return true if modified
         */
        public boolean isModified()
        {
            return modified;
        }

        public static <T> Update<T> noopUpdate()
        {
            return new Update<T>()
            {
                @Override
                public T apply(T original)
                {
                    return original;
                }
            };
        }
    }
}
