package com.basho.riak.client.api.commands.mapreduce;

import com.basho.riak.client.api.commands.mapreduce.filters.EndsWithFilter;
import com.basho.riak.client.api.commands.mapreduce.filters.KeyFilter;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
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

        jg.setCodec(MapReduce.mrObjectMapper);
    }

    @Test
    public void testSearchInputSerializer() throws IOException
    {
        SearchInput input = new SearchInput("index", "query");

        jg.writeObject(input);

        assertEquals("{\"module\":\"yokozuna\",\"function\":\"mapred_search\",\"arg\":[\"index\",\"query\"]}",
                     out.toString());

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
		ArrayList<KeyFilter> filters = new ArrayList<>();
        filters.add(new EndsWithFilter("dave"));
        BucketInput input = new BucketInput(ns, filters);

        jg.writeObject(input);

        assertEquals("{\"bucket\":\"bucket\",\"key_filters\":[[\"ends_with\",\"dave\"]]}", out.toString());

    }

    @Test
    public void testBucketInputSerializerWithType() throws IOException
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
		ArrayList<KeyFilter> filters = new ArrayList<>();
        filters.add(new EndsWithFilter("dave"));
        BucketInput input = new BucketInput(ns, filters);

        jg.writeObject(input);

        assertEquals("{\"bucket\":[\"type\",\"bucket\"],\"key_filters\":[[\"ends_with\",\"dave\"]]}", out.toString());

    }


    
    @Test
    public void testSerializeBucketKeyInput() throws IOException
    {
        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, "bucket");
		ArrayList<BucketKeyInput.IndividualInput> inputs = new ArrayList<>();
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
		ArrayList<BucketKeyInput.IndividualInput> inputs = new ArrayList<>();
        inputs.add(new BucketKeyInput.IndividualInput(new Location(ns, "key")));
        inputs.add(new BucketKeyInput.IndividualInput(new Location(ns, "key"), "data"));
        BucketKeyInput input = new BucketKeyInput(inputs);

        jg.writeObject(input);

        assertEquals("[[\"bucket\",\"key\",\"\",\"type\"],[\"bucket\",\"key\",\"data\",\"type\"]]", out.toString());


    }

    @Test
    public void testSerializeIndexInputMatch() throws Exception
    {
        Namespace ns = new Namespace("bucket");
		IndexInput.MatchCriteria<String> criteria = new IndexInput.MatchCriteria<>("dave");
        IndexInput input = new IndexInput(ns, "index_int", criteria);

        jg.writeObject(input);
        assertEquals("{\"bucket\":\"bucket\",\"index\":\"index_int\",\"key\":\"dave\"}", out.toString());
    }

    @Test
    public void testSerializeIndexInputMatchWithType() throws Exception
    {
        Namespace ns = new Namespace("type", "bucket");
        IndexInput.MatchCriteria<String> criteria = new IndexInput.MatchCriteria<>("dave");
        IndexInput input = new IndexInput(ns, "index_int", criteria);

        jg.writeObject(input);

        assertEquals("{\"bucket\":[\"type\",\"bucket\"],\"index\":\"index_int\",\"key\":\"dave\"}", out.toString());
    }

    @Test
    public void testSerializeIndexInputRange() throws Exception
    {
        Namespace ns = new Namespace("bucket");
		IndexInput.RangeCriteria<Integer> criteria = new IndexInput.RangeCriteria<>(1, 2);
        IndexInput input = new IndexInput(ns, "index_int", criteria);

        jg.writeObject(input);

        assertEquals("{\"bucket\":\"bucket\",\"index\":\"index_int\",\"start\":1,\"end\":2}", out.toString());
    }

    @Test
    public void testSerializeIndexInputRangeWithType() throws Exception
    {
        Namespace ns = new Namespace("type", "bucket");
		IndexInput.RangeCriteria<Integer> criteria = new IndexInput.RangeCriteria<>(1, 2);
        IndexInput input = new IndexInput(ns, "index_int", criteria);

        jg.writeObject(input);

        assertEquals("{\"bucket\":[\"type\",\"bucket\"],\"index\":\"index_int\",\"start\":1,\"end\":2}",
                     out.toString());
    }

}
