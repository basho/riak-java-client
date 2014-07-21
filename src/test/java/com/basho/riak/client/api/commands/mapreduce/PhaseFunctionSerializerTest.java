package com.basho.riak.client.api.commands.mapreduce;

import com.basho.riak.client.api.commands.mapreduce.PhaseFunction;
import com.basho.riak.client.api.commands.mapreduce.PhaseFunctionSerializer;
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

import static org.junit.Assert.assertEquals;

public class PhaseFunctionSerializerTest
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
		objectMapper.registerModule(specModule);

		jg.setCodec(objectMapper);

	}

	@Test
	public void testSerializeErlangFunction() throws IOException
	{
		Function function = Function.newErlangFunction("mod", "fun");
	 	PhaseFunction phaseFunction = new PhaseFunction(function, true);

		jg.writeObject(phaseFunction);

		assertEquals("{\"language\":\"erlang\",\"module\":\"mod\",\"function\":\"fun\",\"keep\":true}", out.toString());
	}

	@Test
	public void testSerializeAnonJsFunction() throws IOException
	{
		Function function = Function.newAnonymousJsFunction("function() {}");
		PhaseFunction phaseFunction = new PhaseFunction(function, true);

		jg.writeObject(phaseFunction);

		assertEquals("{\"language\":\"javascript\",\"source\":\"function() {}\",\"keep\":true}", out.toString());
	}

	@Test
	public void testSerializeNamedJsFunction() throws IOException
	{
		Function function = Function.newNamedJsFunction("the_func");
		PhaseFunction phaseFunction = new PhaseFunction(function, true);

		jg.writeObject(phaseFunction);

		assertEquals("{\"language\":\"javascript\",\"name\":\"the_func\",\"keep\":true}", out.toString());
	}

}
