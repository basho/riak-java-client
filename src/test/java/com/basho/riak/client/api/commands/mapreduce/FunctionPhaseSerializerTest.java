package com.basho.riak.client.api.commands.mapreduce;

import com.basho.riak.client.api.commands.mapreduce.MapPhase;
import com.basho.riak.client.api.commands.mapreduce.ReducePhase;
import com.basho.riak.client.api.commands.mapreduce.PhaseFunction;
import com.basho.riak.client.api.commands.mapreduce.PhaseFunctionSerializer;
import com.basho.riak.client.api.commands.mapreduce.FunctionPhase;
import com.basho.riak.client.api.commands.mapreduce.FunctionPhaseSerializer;
import com.basho.riak.client.core.query.functions.Function;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;

import static junit.framework.Assert.assertEquals;

public class FunctionPhaseSerializerTest
{

	private StringWriter out;
	private JsonGenerator jg;

	@Before
	public void init() throws IOException
	{
		this.out = new StringWriter();
		this.jg = new JsonFactory().createGenerator(out);

		ObjectMapper objectMapper = new ObjectMapper();
		SimpleModule specModule = new SimpleModule("SpecModule", Version.unknownVersion());
		specModule.addSerializer(PhaseFunction.class, new PhaseFunctionSerializer());
		specModule.addSerializer(FunctionPhase.class, new FunctionPhaseSerializer());
		objectMapper.registerModule(specModule);

		jg.setCodec(objectMapper);

	}

	@Test
	public void testSerializeMapPhase() throws IOException
	{

		Function function = Function.newNamedJsFunction("the_func");
		MapPhase phase = new MapPhase(function, "Arg", true);

		jg.writeObject(phase);

		assertEquals("{\"map\":{\"language\":\"javascript\",\"name\":\"the_func\",\"keep\":true,\"arg\":\"Arg\"}}", out.toString());

	}

	@Test
	public void testSerializeReducePhase() throws IOException
	{
		Function function = Function.newNamedJsFunction("the_func");
		ReducePhase phase = new ReducePhase(function, "Arg", true);

		jg.writeObject(phase);

		assertEquals("{\"reduce\":{\"language\":\"javascript\",\"name\":\"the_func\",\"keep\":true,\"arg\":\"Arg\"}}", out.toString());
	}

}
