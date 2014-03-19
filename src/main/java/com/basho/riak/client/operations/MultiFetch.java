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

import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.query.Location;

import java.util.*;
import java.util.concurrent.*;

import static java.util.Collections.unmodifiableList;

/**
 * An operation to fetch multiple values from Riak
 * <p>
 * Riak itself does not support pipelining of requests. MutliFetch addresses this issue by using a threadpool to
 * parallelize a set of fetch operations for a given set of keys.
 * </p>
 * <p>
 * The result of executing this command is a {@code List} of {@link Future} objects, each one representing a single
 * fetch operation. The simplest use would be a loop where you iterate through and wait for them to complete:
 * <p/>
 * <pre>
 * {@code
 * MultiFetch<MyPojo> multifetch = ...;
 * MultiFetch.Response<MyPojo> response = client.execute(multifetch);
 * List<MyPojo> myResults = new ArrayList<MyPojo>();
 * for (Future<FetchValue.Response<MyPojo>> f : response)
 * {
 *     try
 *     {
 *          FetchValue.Response<MyPojo> response = f.get();
 *          myResults.add(response.getValue().get(0));
 *     }
 *     catch (ExecutionException e)
 *     {
 *         // log error, etc.
 *     }
 * }
 * }
 * </pre>
 * </p>
 * <p>
 * <b>Thread Pool:</b><br/>
 * The internal {@link ThreadPoolExecutor} is static; all multi-fetch operations performed by a single instance of the
 * client use the same pool, unless overidden upon construction. This is to prevent resource starvation in the case of
 * multiple simultaneous multi-fetch operations. Idle threads (including core threads) are timed out after 5
 * seconds.
 * <br/><br/>
 * The defaults for {@code corePoolSize} is determined by the Java Runtime using:
 * <br/><br/>
 * {@code Runtime.getRuntime().availableProcessors() * 2;}
 * </p>
 * <p>
 * Be aware that because requests are being parallelized performance is also
 * dependent on the client's underlying connection pool. If there are no connections
 * available performance will suffer initially as connections will need to be established
 * or worse they could time out.
 * </p>
 *
 * @param <T>
 * @author Dave Rusek <drusek@basho.com>
 */
public class MultiFetch<T> extends RiakCommand<MultiFetch.Response<T>>
{

    private final Class<T> convertTo;
    private final ArrayList<Location> keys = new ArrayList<Location>();
	private final Executor executor;
	private final Map<FetchOption<?>, Object> options = new HashMap<FetchOption<?>, Object>();

	private MultiFetch(Builder<T> builder)
	{
		this.keys.addAll(builder.keys);
		this.executor = builder.executor;
		this.options.putAll(builder.options);
        this.convertTo = builder.convertTo;
	}

	@Override
	Response<T> execute(final RiakCluster cluster) throws ExecutionException, InterruptedException
	{

		List<Future<FetchValue.Response<T>>> values =
			new ArrayList<Future<FetchValue.Response<T>>>();

		for (Location key : keys)
		{
			FetchValue.Builder<T> builder = new FetchValue.Builder<T>(key, convertTo);
			
			for (FetchOption<?> option : options.keySet())
			{
				builder.withOption((FetchOption<Object>) option, options.get(option));
			}

			final FetchValue<T> request = builder.build();
			FutureTask<FetchValue.Response<T>> task = new FutureTask<FetchValue.Response<T>>(
				new Callable<FetchValue.Response<T>>()
				{
					@Override
					public FetchValue.Response<T> call() throws Exception
					{
						return request.execute(cluster);
					}
				});

			values.add(task);
			executor.execute(task);

		}


		return new Response<T>(values);

	}

	/**
	 * Build a MultiFetch operation from the supplied parameters. FetchOptions and the converter will be applied,
	 * independently to each fetch operation.
	 *
	 * @param <T> the converted type of the returned objects
	 */
	public static class Builder<T>
	{

		public static final int DEFAULT_POOL_MAX_SIZE = Runtime.getRuntime().availableProcessors() * 2;

		private static final LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();
		private static final ThreadPoolExecutor threadPool =
			new ThreadPoolExecutor(DEFAULT_POOL_MAX_SIZE, DEFAULT_POOL_MAX_SIZE, 5, TimeUnit.SECONDS, workQueue);

		static
		{
			threadPool.allowCoreThreadTimeOut(true);
		}

		private final Class<T> convertTo;
		private ArrayList<Location> keys = new ArrayList<Location>();
		private Executor executor;
		private Map<FetchOption<?>, Object> options = new HashMap<FetchOption<?>, Object>();
        
        /**
         * Construct a Builer for a MutiFetch operation.
         * @param convertTo The class that replies will be converted to.
         */
		public Builder(Class<T> convertTo)
        {
            this.convertTo = convertTo;
        }
        
        /**
		 * Add a key to the list of keys to retrieve as part of this multifetch operation
		 *
		 * @param key key
		 * @return this
		 */
		public Builder withKey(Location key)
		{
			keys.add(key);
			return this;
		}

		/**
		 * Add a list of keys to the list of keys to retrieve as part of this multifetch operation
		 *
		 * @param key vararg list of keys
		 * @return
		 */
		public Builder withKeys(Location... key)
		{
			keys.addAll(Arrays.asList(key));
			return this;
		}

		/**
		 * Add a list of keys to the list of keys to retrieve as part of this multifetch operation
		 *
		 * @param key iterable collection of keys
		 * @return
		 */
		public Builder withKeys(Iterable<Location> key)
		{
			for (Location loc : key)
			{
				keys.add(loc);
			}
			return this;
		}

		/**
		 * Specify an executor on which to run the individual fetch operations. This overrides the internal static
		 * thread pool
		 *
		 * @param executor an executor to use for requests
		 * @return this
		 */
		public Builder withExecutor(Executor executor)
		{
			this.executor = executor;
			return this;
		}

		/**
		 * A {@see FetchOption} to use with each fetch operation
		 *
		 * @param option an option
		 * @param value  the option's associated value
		 * @param <U>    the type of the option's value
		 * @return this
		 */
		public <U> Builder withOption(FetchOption<U> option, U value)
		{
			this.options.put(option, value);
			return this;
		}

		/**
		 * Build a {@see MultiFetch} operation from this builder
		 *
		 * @return an initialized {@see MultiFetch} operation
		 */
		public MultiFetch<T> build()
		{
			if (executor == null)
			{
				executor = threadPool;
			}

			return new MultiFetch<T>(this);
		}

	}

	/**
	 * A result of the execution of this operation, contains a list of individual results for each key
	 *
	 * @param <T> the converted datatype of the returned objects
	 */
	public static final class Response<T> implements Iterable<Future<FetchValue.Response<T>>>
	{

		private final List<Future<FetchValue.Response<T>>> responses;

		Response(List<Future<FetchValue.Response<T>>> responses)
		{
			this.responses = responses;
		}

		@Override
		public Iterator<Future<FetchValue.Response<T>>> iterator()
		{
			return unmodifiableList(responses).iterator();
		}
	}

}
