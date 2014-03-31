package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.query.Location;

public class SearchMapReduce extends MapReduce
{
	protected SearchMapReduce(SearchInput input, Builder builder)
	{
		super(input, builder);
	}

	public static class Builder extends MapReduce.Builder
	{

		private Location bucket;
		private String query;

		@Override
		protected MapReduce.Builder self()
		{
			return this;
		}

		public Builder withBucket(Location bucket)
		{
			this.bucket = bucket;
			return this;
		}

		public Builder withQuery(String query)
		{
			this.query = query;
			return this;
		}

		public SearchMapReduce build()
		{
			if (bucket == null)
			{
				throw new IllegalStateException("A bucket must be specified");
			}

			if (query == null)
			{
				throw new IllegalStateException("A query must be specified");
			}

			return new SearchMapReduce(new SearchInput(bucket, query), this);
		}
	}

}
