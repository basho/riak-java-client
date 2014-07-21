package com.basho.riak.client.api.commands.mapreduce;

import com.basho.riak.client.core.query.functions.Function;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

class PhaseFunctionSerializer extends JsonSerializer<PhaseFunction>
{
	@Override
	public void serialize(PhaseFunction phaseFunction, JsonGenerator jg, SerializerProvider sp) throws IOException
	{
		Function function = phaseFunction.getFunction();

		jg.writeStartObject();

		jg.writeStringField("language", function.isJavascript() ? "javascript" : "erlang");

		if (function.isJavascript())
		{
			if (function.isNamed())
			{
				jg.writeStringField("name", function.getName());
			} else if (function.isStored())
			{
				jg.writeStringField("bucket", function.getBucket());
				jg.writeStringField("key", function.getKey());
			} else if (function.isAnonymous())
			{
				jg.writeStringField("source", function.getSource());
			} else
			{
				throw new IllegalStateException("Cannot determine function type");
			}
		} else if (!function.isJavascript())
		{
			jg.writeStringField("module", function.getModule());
			jg.writeStringField("function", function.getFunction());
		}

		jg.writeBooleanField("keep", phaseFunction.isKeep());

		jg.writeEndObject();
	}
}
