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
package com.basho.riak.client.operations.kv;

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.RiakCommand;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.RiakObject;
import com.fasterxml.jackson.core.type.TypeReference;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Perform an full cycle update of a Riak value: fetch, resolve, modify, store.
 * @author Dave Rusek <drusuk at basho dot com>
 * @since 2.0
 */
public final class UpdateValue extends RiakCommand<UpdateValue.Response>
{
    private final Location location;
    private final Update<?> update;
    private final TypeReference<?> typeReference;
    private final Map<FetchOption<?>, Object> fetchOptions =
	    new HashMap<FetchOption<?>, Object>();
    private final Map<StoreOption<?>, Object> storeOptions =
	    new HashMap<StoreOption<?>, Object>();

    UpdateValue(Builder builder)
    {
        this.location = builder.location;
        this.update = builder.update;
        this.typeReference = builder.typeReference;
	    this.fetchOptions.putAll(builder.fetchOptions);
	    this.storeOptions.putAll(builder.storeOptions);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected final Response doExecute(RiakCluster cluster) throws ExecutionException, InterruptedException
    {

        FetchValue.Builder fetchBuilder = new FetchValue.Builder(location);
        for (Map.Entry<FetchOption<?>, Object> optPair : fetchOptions.entrySet())
        {
            fetchBuilder.withOption((FetchOption<Object>) optPair.getKey(), optPair.getValue());
        }

        FetchValue.Response fetchResponse = fetchBuilder.build().doExecute(cluster);

        Object resolved;
        if (typeReference == null)
        {
            // Steal the type from the Update. Yes, Really.
            ParameterizedType pType = (ParameterizedType)update.getClass().getGenericSuperclass();
            Type t = pType.getActualTypeArguments()[0];
            if (t instanceof ParameterizedType)
            {
                t = ((ParameterizedType)t).getRawType();
            }
        
            resolved = fetchResponse.getValue((Class<?>) t);
        }
        else
        {
            resolved = fetchResponse.getValue(typeReference);
        }
        
        Object updated = ((Update<Object>)update).apply(resolved);

        if (update.isModified())
        {

            StoreValue.Builder store = 
                new StoreValue.Builder(updated, typeReference)
                    .withLocation(location)
                    .withVectorClock(fetchResponse.getVClock());
            
            for (Map.Entry<StoreOption<?>, Object> optPair : storeOptions.entrySet())
            {
                store.withOption((StoreOption<Object>) optPair.getKey(), optPair.getValue());
            }
            StoreValue.Response storeResponse = store.build().doExecute(cluster);

            return new Response.Builder()
                        .withValues(storeResponse.getValues(RiakObject.class))
                        .withLocation(storeResponse.getLocation())
                        .withVClock(storeResponse.getVClock())
                        .withUpdated(true)
                        .build();
        }

        return new Response.Builder()
                .withValues(fetchResponse.getValues(RiakObject.class))
                .withLocation(fetchResponse.getLocation())
                .withVClock(fetchResponse.getVClock())
                .withUpdated(false)
                .build();
    }

    /**
     *
     */
    public static class Response extends KvResponseBase
    {
        private final boolean wasUpdated;

        Response(Init<?> builder)
        {
            super(builder);
            this.wasUpdated = builder.wasUpdated;
        }

        /**
         * Determine if an update occurred.
         * <p>
         * The supplied {@code Update} indicates if a modification was made. If
         * no modification was made, no store operation is performed and this 
         * will return false.
         * <p>
         * @return true if the supplied {@code Update} modified the retrieved object,
         * false otherwise.
         */
        public boolean wasUpdated()
        {
            return wasUpdated;
        }

        protected static abstract class Init<T extends Init<T>> extends KvResponseBase.Init<T>
        {
            private boolean wasUpdated;
            
            T withUpdated(boolean updated)
            {
                this.wasUpdated = updated;
                return self();
            }
        }
        
        static class Builder extends Init<Builder>
        {
            @Override
            protected Builder self()
            {
                return this;
            }

            @Override
            Response build()
            {
                return new Response(this);
            }
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

        /**
         * Returns a no-op Update instance.
         * <p>
         * This can be used to simply resolve siblings that exist 
         * in Riak without modifying the resolved object.
         * <p>
         * 
         * @return An {@code Update} instance the does not modify anything.
         */
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
        private TypeReference<?> typeReference;
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

        /**
         * Supply the Update.
         * <p>
         * During the update operation, the fetched value needs to be converted 
         * before being passed to the {@code ConflictResolver} and the {@code Update}
         * method. 
         * <p>
         * <p>
         * Supplying only an {@code Update<T>} means the raw type of {@code T} 
         * will be used to retrieve the {@code Converter} and {@code ConflictResolver}
         * to be used.
         * </p>
         * @param update The {@code Update} instance
         * @return a reference to this object.
         * @see com.basho.riak.client.convert.Converter
         * @see com.basho.riak.client.convert.ConverterFactory
         * @see com.basho.riak.client.cap.ConflictResolver
         * @see com.basho.riak.client.cap.ConflictResolverFactory
         */
        public Builder withUpdate(Update<?> update)
		{
			this.update = update;
			return this;
		}

        /**
         * Supply the Update with a TypeReference.
         * <p>
         * During the update operation, the fetched value needs to be converted 
         * before being passed to the {@code ConflictResolver} and the {@code Update}
         * method. If your domain object is a parameterized type you will need to supply 
         * a {@code TypeReference} so the appropriate {@code ConflictResolver} 
         * and {@code Converter} can be found.
         * <p>
         * @param update The {@code Update} instance
         * @param typeReference the {@code TypeReference} for the class used for conversion.
         * @return a reference to this object.
         * @see com.basho.riak.client.convert.Converter
         * @see com.basho.riak.client.convert.ConverterFactory
         * @see com.basho.riak.client.cap.ConflictResolver
         * @see com.basho.riak.client.cap.ConflictResolverFactory
         */
        public <T> Builder withUpdate(Update<T> update, TypeReference<T> typeReference)
        {
            this.update = update;
            this.typeReference = typeReference;
            return this;
        }
        
		public UpdateValue build()
		{
			return new UpdateValue(this);
		}
	}
}
