package com.basho.riak.client.operations.mapreduce;

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

public class LinkPhaseSerializerTest
{
	private StringWriter out;
	private JsonGenerator jg;

	@Before
	public void init() throws IOException
	{
		this.out = new StringWriter();
		this.jg = new JsonFactory().createGenerator(out);

		ObjectMapper objectMapper = new ObjectMapper();
		SimpleModule specModule = new SimpleModule("SpecModule", new Version(1, 0, 0, null));
		specModule.addSerializer(LinkPhase.class, new LinkPhaseSerializer());
		objectMapper.registerModule(specModule);

		jg.setCodec(objectMapper);

	}

	@Test
	public void testSerializeLinkPhase() throws IOException
	{
		LinkPhase phase = new LinkPhase("bucket", "tag", true);

		jg.writeObject(phase);

		assertEquals("{\"link\":{\"bucket\":\"bucket\",\"tag\":\"tag\"}}", out.toString());
	}


}
