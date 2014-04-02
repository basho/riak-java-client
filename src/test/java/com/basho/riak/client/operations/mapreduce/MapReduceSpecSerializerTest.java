package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.filters.KeyFilter;
import com.basho.riak.client.query.functions.Function;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;

public class MapReduceSpecSerializerTest
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
		specModule.addSerializer(LinkPhase.class, new LinkPhaseSerializer());
		specModule.addSerializer(FunctionPhase.class, new FunctionPhaseSerializer());
		specModule.addSerializer(BucketInput.class, new BucketInputSerializer());
		specModule.addSerializer(SearchInput.class, new SearchInputSerializer());
		specModule.addSerializer(BucketKeyInput.class, new BucketKeyInputSerializer());
		specModule.addSerializer(IndexInput.class, new IndexInputSerializer());
		objectMapper.registerModule(specModule);

		jg.setCodec(objectMapper);

	}


	@Test
	public void testSerializeMapReduceSpec() throws IOException
	{

		ArrayList<MapReducePhase> phases = new ArrayList<MapReducePhase>();
		phases.add(new MapPhase(Function.newNamedJsFunction("map_func")));
		phases.add(new ReducePhase(Function.newNamedJsFunction("reduce_func")));
		phases.add(new LinkPhase("bucket", "tag"));
		BucketInput input = new BucketInput(new Location("bucket"), Collections.<KeyFilter>emptyList());

		MapReduceSpec spec = new MapReduceSpec(input, phases, 1000L);

		jg.writeObject(spec);
        
		Assert.assertEquals("{\"inputs\":\"bucket\"," +
				"\"timeout\":1000,\"query\":" +
				"[{\"map\":{\"language\":\"javascript\",\"name\":\"map_func\",\"keep\":false,\"arg\":null}}," +
				"{\"reduce\":{\"language\":\"javascript\",\"name\":\"reduce_func\",\"keep\":false,\"arg\":null}}," +
				"{\"link\":{\"bucket\":\"bucket\",\"tag\":\"tag\"}}]}", out.toString());

	}

}
