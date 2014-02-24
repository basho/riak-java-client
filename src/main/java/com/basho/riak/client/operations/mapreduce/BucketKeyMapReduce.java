package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.operations.Location;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BucketKeyMapReduce extends MapReduce
{

	private final List<Input> input = new ArrayList<Input>();

	public BucketKeyMapReduce(Builder builder)
	{
		super(builder);
		this.input.addAll(builder.input);
	}

	private void writeSingleInput(JsonGenerator jg, Input i) throws IOException
	{
		Location location = i.location;
		String keyData = i.keyData;

		jg.writeStartArray();
		jg.writeString(location.getBucket().toString());
		jg.writeString(location.getKey().toString());
		if (i.hasKeyData() || location.hasType())
		{
			jg.writeString(keyData);
		}
		if (location.hasType())
		{
			jg.writeString(location.getType().toString());
		}
		jg.writeEndArray();
	}

	@Override
	protected void writeInput(JsonGenerator jg) throws IOException
	{
		jg.writeStartArray();
		for (Input i : input)
		{
			writeSingleInput(jg, i);
		}
		jg.writeEndArray();
	}

	public static class Builder extends MapReduce.Builder<Builder>
	{

		private List<Input> input = new ArrayList<Input>();

		@Override
		protected Builder self()
		{
			return this;
		}

		public Builder withLocation(Location location)
		{
			input.add(new Input(location));
			return this;
		}

		public Builder withLocation(Location location, String keyData)
		{
			input.add(new Input(location, keyData));
			return this;
		}

		public BucketKeyMapReduce build()
		{
			if (input == null)
			{
				throw new IllegalStateException("At least one location must be specified");
			}

			return new BucketKeyMapReduce(this);
		}

	}

	private static class Input
	{
		public final Location location;
		public final String keyData;

		private Input(Location location, String keyData)
		{
			this.location = location;
			this.keyData = keyData;
		}

		private Input(Location location)
		{
			this.location = location;
			this.keyData = null;
		}

		public boolean hasKeyData()
		{
			return keyData != null;
		}
	}
}
