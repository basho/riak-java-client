package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.query.functions.Function;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

class FunctionPhaseSerializer extends JsonSerializer<FunctionPhase>
{

	private void writePhaseFunction(JsonGenerator jg, FunctionPhase phase) throws IOException
	{
		Function function = phase.getFunction();

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

		jg.writeBooleanField("keep", phase.isKeep());
		jg.writeObjectField("arg", phase.getArg());

		jg.writeEndObject();
	}

	@Override
	public void serialize(FunctionPhase phase, JsonGenerator jg, SerializerProvider sp) throws IOException
	{

		jg.writeStartObject();

		jg.writeFieldName(phase.getType().toString());
		writePhaseFunction(jg, phase);

		jg.writeEndObject();

	}
}
