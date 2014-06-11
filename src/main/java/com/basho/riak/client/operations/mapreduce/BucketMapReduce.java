package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.query.Namespace;
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

		private Namespace namespace;
		private final List<KeyFilter> filters = new ArrayList<KeyFilter>();

		@Override
		protected Builder self()
		{
			return this;
		}

		public Builder withNamespace(Namespace namespace)
		{
			this.namespace = namespace;
			return this;
		}

		public Builder withKeyFilter(KeyFilter filter)
		{
            filters.add(filter);
			return this;
		}
        
		public BucketMapReduce build()
		{
			if (namespace == null)
			{
				throw new IllegalStateException("A Namespace must be specified");
			}

			return new BucketMapReduce(new BucketInput(namespace, filters), this);
		}
	}
}
