package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.operations.Location;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

public class SearchMapReduce extends MapReduce
{
	private final Location bucket;
	private final String query;

	protected SearchMapReduce(Builder builder)
	{
		super(builder);
		this.bucket = builder.bucket;
		this.query = builder.query;
	}

	@Override
	protected void writeInput(JsonGenerator jg) throws IOException
	{

		jg.writeStartObject();
		jg.writeStringField("bucket", bucket.getBucket().toString());
		jg.writeStringField("query", query);
		jg.writeEndObject();

	}

	public static class Builder extends MapReduce.Builder
	{

		private Location bucket;
		private String query;

		@Override
		protected MapReduce.Builder self()
		{
			return this;
		}

		public Builder withBucket(Location bucket)
		{
			this.bucket = bucket;
			return this;
		}

		public Builder withQuery(String query)
		{
			this.query = query;
			return this;
		}

		public SearchMapReduce build()
		{
			if (bucket == null)
			{
				throw new IllegalStateException("A bucket must be specified");
			}

			if (query == null)
			{
				throw new IllegalStateException("A query must be specified");
			}

			return new SearchMapReduce(this);
		}
	}

}
