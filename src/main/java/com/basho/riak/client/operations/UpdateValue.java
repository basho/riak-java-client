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
import com.basho.riak.client.query.Location;

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
    private final Class<T> convertTo;
    private final Location location;
    private final Update<T> update;
    private final ConflictResolver<T> resolver;
    private final Map<FetchOption<?>, Object> fetchOptions =
	    new HashMap<FetchOption<?>, Object>();
    private final Map<StoreOption<?>, Object> storeOptions =
	    new HashMap<StoreOption<?>, Object>();

    UpdateValue(Builder<T> builder)
    {
        this.location = builder.location;
        this.resolver = builder.resolver;
        this.update = builder.update;
	    this.fetchOptions.putAll(builder.fetchOptions);
	    this.storeOptions.putAll(builder.storeOptions);
        this.convertTo = builder.convertTo;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Response<T> execute(RiakCluster cluster) throws ExecutionException, InterruptedException
    {

        FetchValue.Builder<T> fetchBuilder = new FetchValue.Builder<T>(location, convertTo);
        for (Map.Entry<FetchOption<?>, Object> optPair : fetchOptions.entrySet())
        {
            fetchBuilder.withOption((FetchOption<Object>) optPair.getKey(), optPair.getValue());
        }

        FetchValue.Response<T> fetchResponse = fetchBuilder.build().execute(cluster);

        List<T> value = fetchResponse.getValue();
        T resolved = resolver.resolve(value);
        T updated = update.apply(resolved);

        if (update.isModified())
        {

            StoreValue.Builder<T> store = new StoreValue.Builder<T>(location, updated);
            for (Map.Entry<StoreOption<?>, Object> optPair : storeOptions.entrySet())
            {
                store.withOption((StoreOption<Object>) optPair.getKey(), optPair.getValue());
            }
            StoreValue.Response<T> storeResponse = store.build().execute(cluster);

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

	public static class Builder<T>
	{
		private final Location location;
        private final Class<T> convertTo;
		private Update<T> update;
		private ConflictResolver<T> resolver;
		private final Map<FetchOption<?>, Object> fetchOptions =
			new HashMap<FetchOption<?>, Object>();
		private final Map<StoreOption<?>, Object> storeOptions =
			new HashMap<StoreOption<?>, Object>();

		public Builder(Location location, Class<T> convertTo)
		{
			this.location = location;
            this.convertTo = convertTo;
		}

		/**
		 * Add an option for the fetch phase of the update
		 *
		 * @param option the option
		 * @param value  the option's value
		 * @param <U>    the type of the option's value
		 * @return this
		 */
		public <U> Builder<T> withFetchOption(FetchOption<U> option, U value)
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
		public <U> Builder<T> withStoreOption(StoreOption<U> option, U value)
		{
			storeOptions.put(option, value);
			return this;
		}

		public Builder<T> withResolver(ConflictResolver<T> resolver)
		{
			this.resolver = resolver;
			return this;
		}

		public Builder<T> withUpdate(Update<T> update)
		{
			this.update = update;
			return this;
		}

		public UpdateValue<T> build()
		{
			return new UpdateValue<T>(this);
		}
	}
}
