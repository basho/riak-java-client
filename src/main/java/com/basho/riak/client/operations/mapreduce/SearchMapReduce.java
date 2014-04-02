package com.basho.riak.client.operations.mapreduce;

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

		public Builder withIndex(String index)
		{
			this.index = index;
			return this;
		}

		public Builder withQuery(String query)
		{
			this.query = query;
			return this;
		}

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
