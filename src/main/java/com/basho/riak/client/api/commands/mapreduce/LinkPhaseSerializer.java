package com.basho.riak.client.api.commands.mapreduce;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

class LinkPhaseSerializer extends JsonSerializer<LinkPhase>
{
	@Override
	public void serialize(LinkPhase phase, JsonGenerator jg, SerializerProvider sp) throws IOException
	{
		jg.writeStartObject();
		jg.writeFieldName(phase.getType().toString());

		jg.writeStartObject();
		jg.writeStringField("bucket", phase.getBucket());
		jg.writeStringField("tag", phase.getTag());
		jg.writeEndObject();

		jg.writeEndObject();
	}
}
