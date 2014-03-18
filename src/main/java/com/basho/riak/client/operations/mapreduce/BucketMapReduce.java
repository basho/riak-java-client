package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.filter.KeyFilter;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BucketMapReduce extends MapReduce
{

	private final List<KeyFilter> filters = new ArrayList<KeyFilter>();
	private final Location bucket;

	protected BucketMapReduce(Builder builder)
	{
		super(builder);
		this.filters.addAll(builder.filters);
		this.bucket = builder.bucket;
	}

	private void writeKeyFilter(JsonGenerator jg, KeyFilter filter) throws IOException
	{
		jg.writeStartArray();
		for (Object field : filter.asArray())
		{
			jg.writeString(field.toString());
		}
		jg.writeEndArray();
	}

	@Override
	protected void writeInput(JsonGenerator jg) throws IOException
	{
		jg.writeStartObject();
		jg.writeStringField("bucket", bucket.getBucketNameAsString());

		jg.writeArrayFieldStart("key_filters");
		for (KeyFilter filter : filters)
		{
			writeKeyFilter(jg, filter);
		}
		jg.writeEndArray();

		jg.writeEndObject();

	}

	public static class Builder extends MapReduce.Builder<Builder>
	{

		private Location bucket;
		private final List<KeyFilter> filters = new ArrayList<KeyFilter>();

		@Override
		protected Builder self()
		{
			return this;
		}

		public Builder withBucket(Location bucket)
		{
			this.bucket = bucket;
			return this;
		}

		public Builder withKeyFilter(KeyFilter filter)
		{
			filters.add(filter);
			return this;
		}

		public BucketMapReduce build()
		{
			if (bucket == null)
			{
				throw new IllegalStateException("A bucket must be specified");
			}

			return new BucketMapReduce(this);
		}
	}
}
