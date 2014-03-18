package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.query.Location;
import com.basho.riak.client.util.BinaryValue;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

public class IndexMapReduce extends MapReduce
{
	private final Location bucket;
	private final String index;
	private final IndexCriteria criteria;

	protected IndexMapReduce(Builder builder)
	{
		super(builder);
		this.bucket = builder.bucket;
		this.index = builder.index;
		this.criteria = builder.criteria;
	}

	@Override
	protected void writeInput(JsonGenerator jg) throws IOException
	{

		jg.writeStartObject();
		jg.writeStringField("bucket", bucket.getBucketNameAsString());
		jg.writeStringField("index", index);
		criteria.writeFields(jg);
		jg.writeEndObject();

	}

	public static class Builder extends MapReduce.Builder
	{

		private Location bucket;
		private String index;
		private IndexCriteria criteria;

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

		public Builder withIndex(String index)
		{
			this.index = index;
			return this;
		}

		public Builder withRange(final int start, final int end)
		{
			this.criteria = new IndexCriteria()
			{
				@Override
				public void writeFields(JsonGenerator jg) throws IOException
				{
					jg.writeNumberField("start", start);
					jg.writeNumberField("end", end);
				}
			};
			return this;
		}

		public Builder withRange(final BinaryValue start, final BinaryValue end)
		{
			this.criteria = new IndexCriteria()
			{
				@Override
				public void writeFields(JsonGenerator jg) throws IOException
				{
					jg.writeBinaryField("start", start.getValue());
					jg.writeBinaryField("end", end.getValue());
				}
			};
			return this;
		}

		public Builder withMatchValue(final int value)
		{
			this.criteria = new IndexCriteria() {
				@Override
				public void writeFields(JsonGenerator jg) throws IOException
				{
					jg.writeNumberField("key", value);
				}
			};
			return this;
		}

		public Builder withMatchValue(final BinaryValue value)
		{
			this.criteria = new IndexCriteria() {
				@Override
				public void writeFields(JsonGenerator jg) throws IOException
				{
					jg.writeBinaryField("key", value.getValue());
				}
			};
			return this;
		}

		public IndexMapReduce build()
		{

			if (bucket == null)
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

			return new IndexMapReduce(this);
		}

	}

	private static interface IndexCriteria
	{
		void writeFields(JsonGenerator jg) throws IOException;
	}

}
