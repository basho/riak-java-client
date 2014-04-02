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
        jg.writeObjectField("module", "yokozuna");
        jg.writeObjectField("function", "mapred_search");
		
        jg.writeArrayFieldStart("arg");
        jg.writeString(input.getIndex());
		jg.writeString(input.getSearch());
        jg.writeEndArray();
		jg.writeEndObject();
	}
}
