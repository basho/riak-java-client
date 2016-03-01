package com.basho.riak.client.api.commands.mapreduce;

import com.basho.riak.client.api.commands.mapreduce.BucketKeyInputSerializer;
import com.basho.riak.client.api.commands.mapreduce.SearchInputSerializer;
import com.basho.riak.client.api.commands.mapreduce.BucketKeyInput;
import com.basho.riak.client.api.commands.mapreduce.BucketInput;
import com.basho.riak.client.api.commands.mapreduce.IndexInputSerializer;
import com.basho.riak.client.api.commands.mapreduce.SearchInput;
import com.basho.riak.client.api.commands.mapreduce.IndexInput;
import com.basho.riak.client.api.commands.mapreduce.BucketInputSerializer;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.api.commands.mapreduce.filters.EndsWithFilter;
import com.basho.riak.client.api.commands.mapreduce.filters.KeyFilter;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class InputSerializerTest
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
		specModule.addSerializer(BucketInput.class, new BucketInputSerializer());
		specModule.addSerializer(SearchInput.class, new SearchInputSerializer());
		specModule.addSerializer(BucketKeyInput.class, new BucketKeyInputSerializer());
		specModule.addSerializer(IndexInput.class, new IndexInputSerializer());
		objectMapper.registerModule(specModule);

		jg.setCodec(objectMapper);


	}

	@Test
	public void testSearchInputSerializer() throws IOException
	{
		SearchInput input = new SearchInput("index", "query");

		jg.writeObject(input);

		assertEquals("{\"module\":\"yokozuna\",\"function\":\"mapred_search\",\"arg\":[\"index\",\"query\"]}", out.toString());

	}

    @Test
	public void testBucketInputSerializer() throws IOException
    {
        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, "bucket");
        BucketInput input = new BucketInput(ns, null);

		jg.writeObject(input);
        assertEquals("\"bucket\"", out.toString());
    }
    
	@Test
	public void testBucketInputSerializerWithFilter() throws IOException
	{

		Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, "bucket");
		ArrayList<KeyFilter> filters = new ArrayList<KeyFilter>();
		filters.add(new EndsWithFilter("dave"));
		BucketInput input = new BucketInput(ns, filters);

		jg.writeObject(input);

		assertEquals("{\"bucket\":\"bucket\",\"key_filters\":[[\"ends_with\",\"dave\"]]}", out.toString());

	}
    
    @Test public void testBucketInputSerializerWithType() throws IOException
    {
        Namespace ns = new Namespace("type", "bucket");
        BucketInput input = new BucketInput(ns, null);
        jg.writeObject(input);
        assertEquals("[\"type\",\"bucket\"]", out.toString());
    }
    
    @Test
	public void testBucketInputSerializerWithTypeAndFilter() throws IOException
	{

		Namespace ns = new Namespace("type", "bucket");
		ArrayList<KeyFilter> filters = new ArrayList<KeyFilter>();
		filters.add(new EndsWithFilter("dave"));
		BucketInput input = new BucketInput(ns, filters);

		jg.writeObject(input);

		assertEquals("{\"bucket\":[\"type\",\"bucket\"],\"key_filters\":[[\"ends_with\",\"dave\"]]}", out.toString());

	}

    
    
	@Test
	public void testSerializeBucketKeyInput() throws IOException
	{
        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, "bucket");
		ArrayList<BucketKeyInput.IndividualInput> inputs = new ArrayList<BucketKeyInput.IndividualInput>();
		inputs.add(new BucketKeyInput.IndividualInput(new Location(ns, "key")));
		inputs.add(new BucketKeyInput.IndividualInput(new Location(ns, "key"), "data"));
		BucketKeyInput input = new BucketKeyInput(inputs);

		jg.writeObject(input);

		assertEquals("[[\"bucket\",\"key\",\"\"],[\"bucket\",\"key\",\"data\"]]", out.toString());


	}

    @Test
	public void testSerializeBucketKeyInputWithType() throws IOException
	{
        Namespace ns = new Namespace("type", "bucket");
		ArrayList<BucketKeyInput.IndividualInput> inputs = new ArrayList<BucketKeyInput.IndividualInput>();
		inputs.add(new BucketKeyInput.IndividualInput(new Location(ns, "key")));
		inputs.add(new BucketKeyInput.IndividualInput(new Location(ns, "key"), "data"));
		BucketKeyInput input = new BucketKeyInput(inputs);

		jg.writeObject(input);

		assertEquals("[[\"bucket\",\"key\",\"\",\"type\"],[\"bucket\",\"key\",\"data\",\"type\"]]", out.toString());


	}
    
	@Test
	public void testSearializeIndexInputMatch() throws Exception
	{
		Namespace ns = new Namespace("bucket");
		IndexInput.MatchCriteria<String> criteria = new IndexInput.MatchCriteria<String>("dave");
		IndexInput input = new IndexInput(ns, "index_int", criteria);

		jg.writeObject(input);
		assertEquals("{\"bucket\":\"bucket\",\"index\":\"index_int\",\"key\":\"dave\"}", out.toString());
	}
    
    @Test
	public void testSearializeIndexInputMatchWithType() throws Exception
	{
		Namespace ns = new Namespace("type", "bucket");
        IndexInput.MatchCriteria<String> criteria = new IndexInput.MatchCriteria<String>("dave");
		IndexInput input = new IndexInput(ns, "index_int", criteria);

		jg.writeObject(input);

		assertEquals("{\"bucket\":[\"type\",\"bucket\"],\"index\":\"index_int\",\"key\":\"dave\"}", out.toString());
	}

	@Test
	public void testSearializeIndexInputRange() throws Exception
	{
		Namespace ns = new Namespace("bucket");
		IndexInput.RangeCriteria<Integer> criteria = new IndexInput.RangeCriteria<Integer>(1, 2);
		IndexInput input = new IndexInput(ns, "index_int", criteria);

		jg.writeObject(input);

		assertEquals("{\"bucket\":\"bucket\",\"index\":\"index_int\",\"start\":1,\"end\":2}", out.toString());
	}
    
    @Test
	public void testSearializeIndexInputRangeWithType() throws Exception
	{
		Namespace ns = new Namespace("type","bucket");
		IndexInput.RangeCriteria<Integer> criteria = new IndexInput.RangeCriteria<Integer>(1, 2);
		IndexInput input = new IndexInput(ns, "index_int", criteria);

		jg.writeObject(input);

		assertEquals("{\"bucket\":[\"type\",\"bucket\"],\"index\":\"index_int\",\"start\":1,\"end\":2}", out.toString());
	}

}
