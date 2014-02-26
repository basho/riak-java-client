package com.basho.riak.client.operations;

import com.basho.riak.client.operations.datatypes.RiakSet;
import com.basho.riak.client.query.crdt.types.CrdtElement;

public class FetchSet extends FetchDatatype<RiakSet>
{
	private FetchSet(Builder builder)
	{
		super(builder);
	}

	@Override
	public RiakSet extractDatatype(CrdtElement element)
	{
		return new RiakSet(element.getAsSet());
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
