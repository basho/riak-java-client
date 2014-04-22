package com.basho.riak.client.operations.mapreduce;

/**
 * Perform a map-reduce operation on a search query
 * <p/>
 * Basic Usage:
 * <pre>
 *   {@code
 *   Client client = ...
 *   SearchMapReduce mr = new SearchMapReduce.Builder()
 *     .withLocation(new Location("bucket"))
 *     .withIndex("thing_index")
 *     .withRange(1, 100)
 *     .build();
 *   MapReduce.Response response = client.execute(mr);
 *   }
 * </pre>
 */
public class SearchMapReduce extends MapReduce
{
	protected SearchMapReduce(SearchInput input, Builder builder)
	{
		super(input, builder);
	}

	public static class Builder extends MapReduce.Builder<Builder>
	{

		private String index;
		private String query;

		@Override
		protected Builder self()
		{
			return this;
		}

		/**
		 * The bucket to search
		 *
		 * @param bucket the bucket to search
		 * @return this
		 */
		public Builder withBucket(String bucket)
		{
			this.index = bucket;
			return this;
		}

		/**
		 * The query to perform
		 *
		 * @param query the query
		 * @return this
		 */
		public Builder withQuery(String query)
		{
			this.query = query;
			return this;
		}

		/**
		 * Create a new SearchMapReduce request
		 *
		 * @return
		 */
		public SearchMapReduce build()
		{
			if (index == null)
			{
				throw new IllegalStateException("An index must be specified");
			}

			if (query == null)
			{
				throw new IllegalStateException("A query must be specified");
			}

			return new SearchMapReduce(new SearchInput(index, query), this);
		}
	}

}
