package com.basho.riak.client.operations.mapreduce;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class SearchInputSerializer extends JsonSerializer<SearchInput>
{
	@Override
	public void serialize(SearchInput input, JsonGenerator jg, SerializerProvider sp) throws IOException
	{
	 	jg.writeStartObject();
		jg.writeObjectField("bucket", input.getBucket().getBucketNameAsString());
		jg.writeObjectField("query", input.getSearch());
		jg.writeEndObject();
	}
}
