package com.basho.riak.client.operations.mapreduce;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class BucketInputSerializer extends JsonSerializer<BucketInput>
{
	@Override
	public void serialize(BucketInput input, JsonGenerator jg, SerializerProvider sp) throws IOException
	{
		jg.writeStartObject();

		jg.writeObjectField("bucket", input.getBucket().getBucketNameAsString());
		jg.writeObjectField("key_filters", input.getFilters());

		jg.writeEndObject();
	}
}
