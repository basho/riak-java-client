package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.filters.KeyFilter;

import java.util.ArrayList;
import java.util.List;

public class BucketMapReduce extends MapReduce
{

	protected BucketMapReduce(BucketInput input, Builder builder)
	{
		super(input, builder);
	}

	public static class Builder extends MapReduce.Builder<Builder>
	{

		private Location bucket;
		private final List<KeyFilter> filters = new ArrayList<KeyFilter>();

		@Override
		protected Builder self()
		{
			return this;
		}

		public Builder withBucket(Location bucket)
		{
			this.bucket = bucket;
			return this;
		}

		public Builder withKeyFilter(KeyFilter filter)
		{
            filters.add(filter);
			return this;
		}
        
		public BucketMapReduce build()
		{
			if (bucket == null)
			{
				throw new IllegalStateException("A bucket must be specified");
			}

			return new BucketMapReduce(new BucketInput(bucket, filters), this);
		}
	}
}
