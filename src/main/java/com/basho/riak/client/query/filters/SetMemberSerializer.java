package com.basho.riak.client.query.filters;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

class SetMemberSerializer extends JsonSerializer<SetMemberFilter<?>>
{
	@Override
	public void serialize(SetMemberFilter<?> value, JsonGenerator jgen, SerializerProvider provider) throws IOException
	{
		jgen.writeStartArray();
		jgen.writeString(value.getName());
		for (Object member : value.getSet())
		{
			jgen.writeObject(member);
		}
		jgen.writeEndArray();
	}
}
