package com.basho.riak.client.operations;

import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.crdt.types.RiakDatatype;
import com.basho.riak.client.query.crdt.types.RiakSet;

public class FetchSet extends FetchDatatype<RiakSet>
{
	private FetchSet(Builder builder)
	{
		super(builder);
	}

	@Override
	public RiakSet extractDatatype(RiakDatatype element)
	{
		return element.getAsSet();
	}

	public static class Builder extends FetchDatatype.Builder<Builder>
	{

		public Builder(Location location)
		{
			super(location);
		}

		@Override
		protected Builder self()
		{
			return this;
		}

		public FetchSet build()
		{
			return new FetchSet(this);
		}
	}
}
