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
 * @author Dave Rusek <drusuk at basho dot com>
 * @since 2.0
 */
public final class MultiFetch extends RiakCommand<MultiFetch.Response>
{

    private final ArrayList<Location> keys = new ArrayList<Location>();
	private final Executor executor;
	private final Map<FetchOption<?>, Object> options = new HashMap<FetchOption<?>, Object>();

	private MultiFetch(Builder builder)
	{
		this.keys.addAll(builder.keys);
		this.executor = builder.executor;
		this.options.putAll(builder.options);
	}

	@Override
	protected final Response doExecute(final RiakCluster cluster) throws ExecutionException, InterruptedException
	{

		List<Future<FetchValue.Response>> values =
			new ArrayList<Future<FetchValue.Response>>();

		for (Location key : keys)
		{
			FetchValue.Builder builder = new FetchValue.Builder(key);
			
			for (FetchOption<?> option : options.keySet())
			{
				builder.withOption((FetchOption<Object>) option, options.get(option));
			}

			final FetchValue request = builder.build();
			FutureTask<FetchValue.Response> task = new FutureTask<FetchValue.Response>(
				new Callable<FetchValue.Response>()
				{
					@Override
					public FetchValue.Response call() throws Exception
					{
						return request.doExecute(cluster);
					}
				});

			values.add(task);
			executor.execute(task);

		}


		return new Response(values);

	}

	/**
	 * Build a MultiFetch operation from the supplied parameters. FetchOptions and the converter will be applied,
	 * independently to each fetch operation.
	 *
	 * @param <T> the converted type of the returned objects
	 */
	public static class Builder
	{

		public static final int DEFAULT_POOL_MAX_SIZE = Runtime.getRuntime().availableProcessors() * 2;

		private static final LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();
		private static final ThreadPoolExecutor threadPool =
			new ThreadPoolExecutor(DEFAULT_POOL_MAX_SIZE, DEFAULT_POOL_MAX_SIZE, 5, TimeUnit.SECONDS, workQueue);

		static
		{
			threadPool.allowCoreThreadTimeOut(true);
		}

		private ArrayList<Location> keys = new ArrayList<Location>();
		private Executor executor;
		private Map<FetchOption<?>, Object> options = new HashMap<FetchOption<?>, Object>();
        
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
		public MultiFetch build()
		{
			if (executor == null)
			{
				executor = threadPool;
			}

			return new MultiFetch(this);
		}

	}

	/**
	 * A result of the execution of this operation, contains a list of individual results for each key
	 *
	 */
	public static final class Response implements Iterable<Future<FetchValue.Response>>
	{

		private final List<Future<FetchValue.Response>> responses;

		Response(List<Future<FetchValue.Response>> responses)
		{
			this.responses = responses;
		}

		@Override
		public Iterator<Future<FetchValue.Response>> iterator()
		{
			return unmodifiableList(responses).iterator();
		}
	}

}
