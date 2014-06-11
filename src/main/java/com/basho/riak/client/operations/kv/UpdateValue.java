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
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.convert.reflection.AnnotationUtil;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakFutureListener;
import com.basho.riak.client.operations.ListenableFuture;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.RiakObject;
import com.fasterxml.jackson.core.type.TypeReference;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Perform an full cycle update of a Riak value: fetch, resolve, modify, store.
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class UpdateValue extends RiakCommand<UpdateValue.Response, Location>
{
    private final Location location;
    private final Update<?> update;
    private final TypeReference<?> typeReference;
    private final Map<FetchValue.Option<?>, Object> fetchOptions =
	    new HashMap<FetchValue.Option<?>, Object>();
    private final Map<StoreValue.Option<?>, Object> storeOptions =
	    new HashMap<StoreValue.Option<?>, Object>();

    UpdateValue(Builder builder)
    {
        this.location = builder.location;
        this.update = builder.update;
        this.typeReference = builder.typeReference;
	    this.fetchOptions.putAll(builder.fetchOptions);
	    this.storeOptions.putAll(builder.storeOptions);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RiakFuture<Response, Location> executeAsync(final RiakCluster cluster)
    {
        final UpdateValueFuture updateFuture = new UpdateValueFuture(location);
        
        FetchValue.Builder fetchBuilder = new FetchValue.Builder(location);
        for (Map.Entry<FetchValue.Option<?>, Object> optPair : fetchOptions.entrySet())
        {
            fetchBuilder.withOption((FetchValue.Option<Object>) optPair.getKey(), optPair.getValue());
        }

        RiakFuture<FetchValue.Response, Location> fetchFuture =
            fetchBuilder.build().executeAsync(cluster);
        
        // Anonymous listener that will do the work
        RiakFutureListener<FetchValue.Response, Location> fetchListener =
            new RiakFutureListener<FetchValue.Response, Location>()
            {
                @Override
                public void handle(RiakFuture<FetchValue.Response, Location> f)
                {
                    if (f.isSuccess())
                    {
                        FetchValue.Response fetchResponse;
                        try 
                        {
                            fetchResponse = f.get();
                            Object resolved = null;
                            VClock vclock = null;
                            
                            if (!fetchResponse.isNotFound())
                            {
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

                                // We get the vclock so we can inject it into the updated object. 
                                // This is so the end user doesn't have to worry about vclocks
                                // in the Update.
                                vclock = fetchResponse.getVectorClock();
                            }
                            
                            Object updated = ((Update<Object>)update).apply(resolved);

                            if (update.isModified())
                            {
                                AnnotationUtil.setVClock(updated, vclock);
                                
                                StoreValue.Builder store = 
                                    new StoreValue.Builder(updated, typeReference)
                                        .withLocation(location)
                                        .withVectorClock(vclock);

                                for (Map.Entry<StoreValue.Option<?>, Object> optPair : storeOptions.entrySet())
                                {
                                    store.withOption((StoreValue.Option<Object>) optPair.getKey(), optPair.getValue());
                                }
                                RiakFuture<StoreValue.Response, Location> storeFuture = 
                                    store.build().executeAsync(cluster);
                                storeFuture.addListener(updateFuture);
                            }
                            else
                            {
                                Response updateResponse = new Response.Builder()
                                    .withLocation(f.getQueryInfo())
                                    .withUpdated(false)
                                    .build();
                                updateFuture.setResponse(updateResponse);
                            }
                            
                            
                        }
                        catch (InterruptedException ex) 
                        {
                            updateFuture.setException(ex);
                        }
                        catch (UnresolvedConflictException ex)
                        {
                            updateFuture.setException(ex);
                        }
                        catch (ConversionException ex)
                        {
                            updateFuture.setException(ex);
                        }
                    }
                    else
                    {
                        updateFuture.setException(f.cause().getCause());
                    }
                }
            };
        
        fetchFuture.addListener(fetchListener);
        return updateFuture;
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

        /**
         * @ExcludeFromJavadoc 
         */
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
        
        /**
         * Returns an Update that replaces whatever is in Riak.
         * <p>
         * This Update simply returns the supplied value, thus replacing 
         * anything currently in Riak with that value.
         * </p>
         * @param newValue the obj to store in Riak.
         * @return an Update instance.
         */
        public static <T> Update<T> clobberUpdate(final T newValue)
        {
            return new Update<T>()
            {
                @Override
                public T apply(T original)
                {
                    return newValue;
                }
                
            };
        }
    }

	public static class Builder
	{
		private final Location location;
		private Update<?> update;
        private TypeReference<?> typeReference;
		private final Map<FetchValue.Option<?>, Object> fetchOptions =
			new HashMap<FetchValue.Option<?>, Object>();
		private final Map<StoreValue.Option<?>, Object> storeOptions =
			new HashMap<StoreValue.Option<?>, Object>();

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
		public <U> Builder withFetchOption(FetchValue.Option<U> option, U value)
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
		public <U> Builder withStoreOption(StoreValue.Option<U> option, U value)
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
        
        /**
         * Set the Riak-side timeout value.
         * <p>
         * By default, riak has a 60s timeout for operations. Setting
         * this value will override that default for both the 
         * fetch and store operation.
         * </p>
         * @param timeout the timeout in milliseconds to be sent to riak.
         * @return a reference to this object.
         */
        public Builder withTimeout(int timeout)
        {
            withFetchOption(FetchValue.Option.TIMEOUT, timeout);
            withStoreOption(StoreValue.Option.TIMEOUT, timeout);
            return this;
        }
        
		public UpdateValue build()
		{
			return new UpdateValue(this);
		}
	}
    
    private class UpdateValueFuture extends ListenableFuture<UpdateValue.Response, Location>
        implements RiakFutureListener<StoreValue.Response, Location>
    {
        private final Location location;
        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile Throwable exception;
        private volatile Response updateResponse;
        
        private UpdateValueFuture(Location location)
        {
            this.location = location;
        }
        
        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            return false;
        }

        @Override
        public Response get() throws InterruptedException
        {
            latch.await();
            return updateResponse;
        }

        @Override
        public Response get(long timeout, TimeUnit unit) throws InterruptedException
        {
            latch.await(timeout, unit);
            return updateResponse;
        }

        @Override
        public boolean isCancelled()
        {
            return false;
        }

        @Override
        public boolean isDone()
        {
            return latch.getCount() != 1;
        }

        @Override
        public void await() throws InterruptedException
        {
            latch.await();
        }

        @Override
        public void await(long timeout, TimeUnit unit) throws InterruptedException
        {
            latch.await(timeout, unit);
        }

        @Override
        public boolean isSuccess()
        {
            return isDone() && exception == null;
        }

        @Override
        public Throwable cause()
        {
            return exception;
        }

        private void setResponse(Response response)
        {
            this.updateResponse = response;
            latch.countDown();
            notifyListeners();
        }
        
        private void setException(Throwable t)
        {
            this.exception = t;
            latch.countDown();
            notifyListeners();
        }

        @Override
        public void handle(RiakFuture<StoreValue.Response, Location> f)
        {
            if (f.isSuccess())
            {
                StoreValue.Response storeResponse;
                try 
                {
                    storeResponse = f.get();
                    Response response = new Response.Builder()
                        .withValues(storeResponse.getValues(RiakObject.class))
                        .withLocation(f.getQueryInfo())
                        .withUpdated(true)
                        .build();
                    setResponse(response);
                    
                }
                catch (InterruptedException ex) 
                {
                    setException(ex);
                }
            }
            else
            {
                setException(f.cause().getCause());
            }
        }

        @Override
        public Location getQueryInfo()
        {
            return location;
        }
    }
}
