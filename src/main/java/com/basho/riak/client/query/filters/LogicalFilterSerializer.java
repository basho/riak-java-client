package com.basho.riak.client.query.filters;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

class LogicalFilterSerializer extends JsonSerializer<LogicalFilter>
{
	@Override
	public void serialize(LogicalFilter value, JsonGenerator jgen, SerializerProvider provider) throws IOException
	{
		jgen.writeStartArray();
		jgen.writeString(value.getName());
		for (KeyFilter filter : value.getFilters())
		{
			jgen.writeStartArray();
			jgen.writeObject(filter);
			jgen.writeEndArray();
		}
		jgen.writeEndArray();
	}
}
