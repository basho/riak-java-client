package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.filters.KeyFilter;

import java.util.ArrayList;
import java.util.List;

/**
 * Perform a map-reduce operation on a bucket
 * <p/>
 * Basic Usage:
 * <pre>
 *   {@code
 *   Client client = ...
 *   BucketMapReduce mr = new BucketMapReduce.Builder()
 *     .withLocation(new Location("bucket"))
 *     .build();
 *   MapReduce.Response response = client.execute(mr);
 *   }
 * </pre>
 */
public class BucketMapReduce extends MapReduce
{

	protected BucketMapReduce(BucketInput input, Builder builder)
	{
		super(input, builder);
	}

	public static class Builder extends MapReduce.Builder<Builder>
	{

		private Location location;
		private final List<KeyFilter> filters = new ArrayList<KeyFilter>();

		@Override
		protected Builder self()
		{
			return this;
		}

		/**
		 * Add a location to the list of locations to use as MR input.
		 *
		 * @param location a location
		 * @return this
		 */
		public Builder withLocation(Location location)
		{
			this.location = location;
			return this;
		}

		/**
		 * Add a filter to the list of filters to apply to keys before they are passed as input
		 * to the MR job.
		 *
		 * @param filter a {@link com.basho.riak.client.query.filters.KeyFilter}
		 * @return this
		 */
		public Builder withKeyFilter(KeyFilter filter)
		{
            filters.add(filter);
			return this;
		}

		/**
		 * Create a new BucketMapReduce request
		 *
		 * @return new request
		 */
		public BucketMapReduce build()
		{
			if (location == null)
			{
				throw new IllegalStateException("A bucket must be specified");
			}

			return new BucketMapReduce(new BucketInput(location, filters), this);
		}
	}
}
