package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.filters.EndsWithFilter;
import com.basho.riak.client.query.filters.KeyFilter;
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

import static junit.framework.Assert.assertEquals;

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

		Location bucket = new Location("bucket");
		SearchInput input = new SearchInput(bucket, "query");

		jg.writeObject(input);

		assertEquals("{\"bucket\":\"bucket\",\"query\":\"query\"}", out.toString());

	}

    @Test
	public void testBucketInputSerializer() throws IOException
    {
        Location bucket = new Location("bucket");
        BucketInput input = new BucketInput(bucket, null);

		jg.writeObject(input);
        assertEquals("\"bucket\"", out.toString());
    }
    
	@Test
	public void testBucketInputSerializerWithFilter() throws IOException
	{

		Location bucket = new Location("bucket");
		ArrayList<KeyFilter> filters = new ArrayList<KeyFilter>();
		filters.add(new EndsWithFilter("dave"));
		BucketInput input = new BucketInput(bucket, filters);

		jg.writeObject(input);

		assertEquals("{\"bucket\":\"bucket\",\"key_filters\":[[\"ends_with\",\"dave\"]]}", out.toString());

	}
    
    @Test public void testBucketInputSerializerWithType() throws IOException
    {
        Location bucket = new Location("bucket").setBucketType("type");
        BucketInput input = new BucketInput(bucket, null);
        jg.writeObject(input);
        assertEquals("[\"type\",\"bucket\"]", out.toString());
    }
    
    @Test
	public void testBucketInputSerializerWithTypeAndFilter() throws IOException
	{

		Location bucket = new Location("bucket").setBucketType("type");
		ArrayList<KeyFilter> filters = new ArrayList<KeyFilter>();
		filters.add(new EndsWithFilter("dave"));
		BucketInput input = new BucketInput(bucket, filters);

		jg.writeObject(input);

		assertEquals("{\"bucket\":[\"type\",\"bucket\"],\"key_filters\":[[\"ends_with\",\"dave\"]]}", out.toString());

	}

    
    
	@Test
	public void testSerializeBucketKeyInput() throws IOException
	{

		ArrayList<BucketKeyInput.IndividualInput> inputs = new ArrayList<BucketKeyInput.IndividualInput>();
		inputs.add(new BucketKeyInput.IndividualInput(new Location("bucket").setKey("key")));
		inputs.add(new BucketKeyInput.IndividualInput(new Location("bucket").setKey("key"), "data"));
		BucketKeyInput input = new BucketKeyInput(inputs);

		jg.writeObject(input);

		assertEquals("[[\"bucket\",\"key\",\"\"],[\"bucket\",\"key\",\"data\"]]", out.toString());


	}

    @Test
	public void testSerializeBucketKeyInputWithType() throws IOException
	{

		ArrayList<BucketKeyInput.IndividualInput> inputs = new ArrayList<BucketKeyInput.IndividualInput>();
		inputs.add(new BucketKeyInput.IndividualInput(new Location("bucket").setBucketType("type").setKey("key")));
		inputs.add(new BucketKeyInput.IndividualInput(new Location("bucket").setBucketType("type").setKey("key"), "data"));
		BucketKeyInput input = new BucketKeyInput(inputs);

		jg.writeObject(input);

		assertEquals("[[\"bucket\",\"key\",\"\",\"type\"],[\"bucket\",\"key\",\"data\",\"type\"]]", out.toString());


	}
    
	@Test
	public void testSearializeIndexInputMatch() throws Exception
	{
		Location bucket = new Location("bucket");
		IndexInput.MatchCriteria<String> criteria = new IndexInput.MatchCriteria<String>("dave");
		IndexInput input = new IndexInput(bucket, "index_int", criteria);

		jg.writeObject(input);

		assertEquals("{\"bucket\":\"bucket\",\"index\":\"index_int\",\"key\":\"dave\"}", out.toString());
	}

	@Test
	public void testSearializeIndexInputRange() throws Exception
	{
		Location bucket = new Location("bucket");
		IndexInput.RangeCriteria<Integer> criteria = new IndexInput.RangeCriteria<Integer>(1, 2);
		IndexInput input = new IndexInput(bucket, "index_int", criteria);

		jg.writeObject(input);

		assertEquals("{\"bucket\":\"bucket\",\"index\":\"index_int\",\"start\":1,\"end\":2}", out.toString());
	}

}
