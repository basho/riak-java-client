package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.query.Location;
import com.basho.riak.client.util.BinaryValue;

public class IndexMapReduce extends MapReduce
{
	protected IndexMapReduce(IndexInput input, Builder builder)
	{
		super(input, builder);
	}

	public static class Builder extends MapReduce.Builder<Builder>
	{

		private Location location;
		private String index;
		private IndexInput.IndexCriteria criteria;

		@Override
		protected Builder self()
		{
			return this;
		}

		public Builder withLocation(Location location)
		{
			this.location = location;
			return this;
		}

		public Builder withIndex(String index)
		{
			this.index = index;
			return this;
		}

		public Builder withRange(final long start, final long end)
		{
			this.criteria = new IndexInput.RangeCriteria<Long>(start, end);
			return this;
		}

		public Builder withRange(final BinaryValue start, final BinaryValue end)
		{
			this.criteria = new IndexInput.RangeCriteria<BinaryValue>(start, end);
			return this;
		}

		public Builder withMatchValue(final long value)
		{
			this.criteria = new IndexInput.MatchCriteria<Long>(value);
			return this;
		}

		public Builder withMatchValue(final BinaryValue value)
		{
			this.criteria = new IndexInput.MatchCriteria<BinaryValue>(value);
			return this;
		}

		public IndexMapReduce build()
		{

			if (location == null)
			{
				throw new IllegalStateException("A bucket must be specified");
			}

			if (index == null)
			{
				throw new IllegalStateException("An index must be specified");
			}

			if (criteria == null)
			{
				throw new IllegalStateException("An index search criteria must be specified");
			}

			return new IndexMapReduce(new IndexInput(location, index, criteria), this);
		}

	}

}
