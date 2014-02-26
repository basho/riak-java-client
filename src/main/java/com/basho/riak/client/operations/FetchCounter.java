package com.basho.riak.client.operations;

import com.basho.riak.client.operations.datatypes.RiakCounter;
import com.basho.riak.client.query.crdt.types.CrdtElement;

public class FetchCounter extends FetchDatatype<RiakCounter>
{
	private FetchCounter(Builder builder)
	{
		super(builder);
	}

	@Override
	public RiakCounter extractDatatype(CrdtElement element)
	{
		return new RiakCounter(element.getAsCounter());
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
