package com.basho.riak.client.operations;

import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.crdt.types.RiakCounter;
import com.basho.riak.client.query.crdt.types.RiakDatatype;

public class FetchCounter extends FetchDatatype<RiakCounter>
{
	private FetchCounter(Builder builder)
	{
		super(builder);
	}

	@Override
	public RiakCounter extractDatatype(RiakDatatype element)
	{
		return element.getAsCounter();
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

		public FetchCounter build()
		{
			return new FetchCounter(this);
		}
	}
}
