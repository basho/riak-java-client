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
import com.basho.riak.client.cap.ConflictResolverFactory;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.convert.ConverterFactory;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.RiakObject;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Perform an full cycle update of a Riak value: fetch, resolve, modify, store.
 *
 * @param <T> the datatype that is being operated on
 */
public class UpdateValue extends RiakCommand<UpdateValue.Response>
{
    private final Location location;
    private final Update<?> update;
    private final Map<FetchOption<?>, Object> fetchOptions =
	    new HashMap<FetchOption<?>, Object>();
    private final Map<StoreOption<?>, Object> storeOptions =
	    new HashMap<StoreOption<?>, Object>();

    UpdateValue(Builder builder)
    {
        this.location = builder.location;
        this.update = builder.update;
	    this.fetchOptions.putAll(builder.fetchOptions);
	    this.storeOptions.putAll(builder.storeOptions);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Response execute(RiakCluster cluster) throws ExecutionException, InterruptedException
    {

        FetchValue.Builder fetchBuilder = new FetchValue.Builder(location);
        for (Map.Entry<FetchOption<?>, Object> optPair : fetchOptions.entrySet())
        {
            fetchBuilder.withOption((FetchOption<Object>) optPair.getKey(), optPair.getValue());
        }

        FetchValue.Response fetchResponse = fetchBuilder.build().execute(cluster);

        // Steal the type from the Update. Yes, Really.
        Class<?> clazz = 
            (Class<?>)((ParameterizedType)update.getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        
        Object resolved = fetchResponse.getValue(clazz);
        Object updated = ((Update<Object>)update).apply(resolved);

        if (update.isModified())
        {

            StoreValue.Builder store = 
                new StoreValue.Builder(location, updated)
                    .withVectorClock(fetchResponse.getvClock());
            
            for (Map.Entry<StoreOption<?>, Object> optPair : storeOptions.entrySet())
            {
                store.withOption((StoreOption<Object>) optPair.getKey(), optPair.getValue());
            }
            StoreValue.Response storeResponse = store.build().execute(cluster);

            List<RiakObject> values = storeResponse.getValues(RiakObject.class);
            VClock clock = storeResponse.getvClock();

            return new Response(values, storeResponse.getLocation(), clock);

        }

        return new Response(fetchResponse.getValues(RiakObject.class), 
                                fetchResponse.getLocation(), 
                                fetchResponse.getvClock());
    }

    /**
     *
     */
    public static class Response
    {
        private final Location location;
        private final VClock vClock;
        private final List<RiakObject> values;

        Response(List<RiakObject> values, Location location, VClock vClock)
        {
            this.values = values;
            this.vClock = vClock;
            this.location = location;
        }

        public VClock getvClock()
        {
            return vClock;
        }

        public <T> T getValue(Class<T> clazz) throws UnresolvedConflictException
        {
            List<T> converted = convertValues(clazz);
            ConflictResolver<T> resolver = 
                ConflictResolverFactory.getInstance().getConflictResolverForClass(clazz);
            
            return resolver.resolve(converted);
        }
        
        public <T> List<T> getValues(Class<T> clazz)
        {
            return convertValues(clazz);
        }
        
        public Location getLocation()
        {
            return location;
        }
        
        private <T> List<T> convertValues(Class<T> clazz)
        {
            Converter<T> converter = ConverterFactory.getInstance().getConverterForClass(clazz);
            
            List<T> convertedValues = new ArrayList<T>(values.size());
            for (RiakObject ro : values)
            {
                convertedValues.add(converter.toDomain(ro, location, vClock));
            }
            
            return convertedValues;
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

	public static class Builder
	{
		private final Location location;
		private Update<?> update;
		private final Map<FetchOption<?>, Object> fetchOptions =
			new HashMap<FetchOption<?>, Object>();
		private final Map<StoreOption<?>, Object> storeOptions =
			new HashMap<StoreOption<?>, Object>();

		public Builder(Location location)
		{
			this.location = location;
		}

		/**
		 * Add an option for the fetch phase of the update
		 *
		 * @param option the option
		 * @param value  the option's value
		 * @param <U>    the type of the option's value
		 * @return this
		 */
		public <U> Builder withFetchOption(FetchOption<U> option, U value)
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
		public <U> Builder withStoreOption(StoreOption<U> option, U value)
		{
			storeOptions.put(option, value);
			return this;
		}

        public Builder withUpdate(Update<?> update)
		{
			this.update = update;
			return this;
		}

		public UpdateValue build()
		{
			return new UpdateValue(this);
		}
	}
}
