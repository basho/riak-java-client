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

import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.RiakCommand;
import com.basho.riak.client.core.FailureInfo;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.operations.CoreFutureAdapter;
import com.basho.riak.client.operations.RiakOption;
import com.basho.riak.client.query.Location;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;


/**
 * Command used to fetch a value from Riak, referenced by it's key.
 *
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class FetchValue extends RiakCommand<FetchValue.Response, Location>
{

	private final Location location;
	private final Map<FetchOption<?>, Object> options =
			new HashMap<FetchOption<?>, Object>();

	FetchValue(Builder builder)
	{
		this.location = builder.location;
		this.options.putAll(builder.options);
	}

	@Override
	protected final Response doExecute(RiakCluster cluster) throws ExecutionException, InterruptedException
	{
		RiakFuture<Response, Location> future = doExecuteAsync(cluster);

		future.await();

		if (future.isSuccess())
		{
			return future.get();
		} else
		{
			throw new ExecutionException(future.cause().getCause());
		}
	}

	@Override
	protected final RiakFuture<Response, Location> doExecuteAsync(RiakCluster cluster)
	{
		RiakFuture<FetchOperation.Response, Location> coreFuture =
				cluster.execute(buildCoreOperation());

		CoreFutureAdapter<Response, Location, FetchOperation.Response, Location> future =
				new CoreFutureAdapter<Response, Location, FetchOperation.Response, Location>(coreFuture)
				{
					@Override
					protected Response convertResponse(FetchOperation.Response coreResponse)
					{
						return new Response.Builder().withNotFound(coreResponse.isNotFound())
								.withUnchanged(coreResponse.isUnchanged())
								.withLocation(coreResponse.getLocation())
								.withValues(coreResponse.getObjectList())
								.withVClock(coreResponse.getVClock())
								.build();
					}

					@Override
					protected FailureInfo<Location> convertFailureInfo(FailureInfo<Location> coreQueryInfo)
					{
						return coreQueryInfo;
					}
				};
		coreFuture.addListener(future);
		return future;

	}

	private FetchOperation buildCoreOperation()
	{
		FetchOperation.Builder builder = new FetchOperation.Builder(location);

		for (Map.Entry<FetchOption<?>, Object> opPair : options.entrySet())
		{

			RiakOption<?> option = opPair.getKey();

			if (option == FetchOption.R)
			{
				builder.withR(((Quorum) opPair.getValue()).getIntValue());
			} else if (option == FetchOption.DELETED_VCLOCK)
			{
				builder.withReturnDeletedVClock((Boolean) opPair.getValue());
			} else if (option == FetchOption.TIMEOUT)
			{
				builder.withTimeout((Integer) opPair.getValue());
			} else if (option == FetchOption.HEAD)
			{
				builder.withHeadOnly((Boolean) opPair.getValue());
			} else if (option == FetchOption.BASIC_QUORUM)
			{
				builder.withBasicQuorum((Boolean) opPair.getValue());
			} else if (option == FetchOption.IF_MODIFIED)
			{
				VClock clock = (VClock) opPair.getValue();
				builder.withIfNotModified(clock.getBytes());
			} else if (option == FetchOption.N_VAL)
			{
				builder.withNVal((Integer) opPair.getValue());
			} else if (option == FetchOption.PR)
			{
				builder.withPr(((Quorum) opPair.getValue()).getIntValue());
			} else if (option == FetchOption.SLOPPY_QUORUM)
			{
				builder.withSloppyQuorum((Boolean) opPair.getValue());
			} else if (option == FetchOption.NOTFOUND_OK)
			{
				builder.withNotFoundOK((Boolean) opPair.getValue());
			}

		}

		return builder.build();
	}

	/**
	 * A response from Riak including the vector clock.
	 */
	public static class Response extends KvResponseBase
	{
		private final boolean notFound;
		private final boolean unchanged;

		Response(Init<?> builder)
		{
			super(builder);
			this.notFound = builder.notFound;
			this.unchanged = builder.unchanged;
		}

		/**
		 * Determine if there was a value in Riak.
		 * <p>
		 * If there was no value present at the supplied {@code Location} in
		 * Riak, this will be true.
		 * </p>
		 *
		 * @return true if there was no value in Riak.
		 */
		public boolean isNotFound()
		{
			return notFound;
		}

		/**
		 * Determine if the value is unchanged.
		 * <p/>
		 * If the fetch request set {@link com.basho.riak.client.operations.kv.FetchOption#IF_MODIFIED}
		 * this indicates if the value in Riak has been modified.
		 * <p/>
		 *
		 * @return true if the vector clock for the object in Riak matched the
		 * supplied vector clock, false otherwise.
		 */
		public boolean isUnchanged()
		{
			return unchanged;
		}

		protected static abstract class Init<T extends Init<T>> extends KvResponseBase.Init<T>
		{
			private boolean notFound;
			private boolean unchanged;

			T withUnchanged(boolean unchanged)
			{
				this.unchanged = unchanged;
				return self();
			}

			T withNotFound(boolean notFound)
			{
				this.notFound = notFound;
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

	public static class Builder
	{

		private final Location location;
		private final Map<FetchOption<?>, Object> options =
				new HashMap<FetchOption<?>, Object>();

		public Builder(Location location)
		{
			this.location = location;
		}

		/**
		 * Add an optional setting for this command. This will be passed along with the request to Riak to tell it how
		 * to behave when servicing the request.
		 *
		 * @param option
		 * @param value
		 * @param <U>
		 * @return
		 */
		public <U> Builder withOption(FetchOption<U> option, U value)
		{
			options.put(option, value);
			return this;
		}

		/**
		 * Build a {@link FetchValue} object
		 *
		 * @return a FetchValue command
		 */
		public FetchValue build()
		{
			return new FetchValue(this);
		}
	}
}
