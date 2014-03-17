package com.basho.riak.client.operations;

import com.basho.riak.client.operations.datatypes.RiakMap;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.crdt.types.CrdtElement;

public class FetchMap extends FetchDatatype<RiakMap>
{
	private FetchMap(Builder builder)
	{
		super(builder);
	}

	@Override
	public RiakMap extractDatatype(CrdtElement element)
	{
		return new RiakMap(element.getAsMap());
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

		public FetchMap build()
		{
			return new FetchMap(this);
		}

	}

}
