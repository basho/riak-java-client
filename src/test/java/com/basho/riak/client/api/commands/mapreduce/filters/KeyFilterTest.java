package com.basho.riak.client.api.commands.mapreduce.filters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;

public class KeyFilterTest
{

    private ObjectWriter writer;
    private StringWriter serialized;

    @Before
    public void init()
    {
        ObjectMapper mapper = new ObjectMapper();
        writer = mapper.writer();
        serialized = new StringWriter();
    }

    @Test
    public void testGreaterThanInt() throws IOException
    {
        GreaterThanFilter<Integer> filter = new GreaterThanFilter<>(1);

        writer.writeValue(serialized, filter);
        assertEquals("[\"greater_than\",1]", serialized.toString());

    }

    @Test
    public void testLessThanInt() throws Exception
    {
        LessThanFilter<Integer> filter = new LessThanFilter<>(1);

        writer.writeValue(serialized, filter);
        assertEquals("[\"less_than\",1]", serialized.toString());
    }

    @Test
    public void testGreaterThanEqInt() throws IOException
    {
        GreaterThanOrEqualFilter<Integer> filter = new GreaterThanOrEqualFilter<>(1);

        writer.writeValue(serialized, filter);
        assertEquals("[\"greater_than_eq\",1]", serialized.toString());

    }

    @Test
    public void testLessThanEqInt() throws Exception
    {
        LessThanOrEqualFilter<Integer> filter = new LessThanOrEqualFilter<>(1);

        writer.writeValue(serialized, filter);
        assertEquals("[\"less_than_eq\",1]", serialized.toString());
    }

    @Test
    public void testBetweenInt() throws Exception
    {
        BetweenFilter<Integer> filter = new BetweenFilter<>(1, 2);

        writer.writeValue(serialized, filter);
        assertEquals("[\"between\",1,2]", serialized.toString());
    }

    @Test
    public void testMatches() throws IOException
    {
        MatchFilter filter = new MatchFilter("string1");

        writer.writeValue(serialized, filter);
        assertEquals("[\"matches\",\"string1\"]", serialized.toString());
    }

    @Test
    public void testNeqInt() throws IOException
    {
        NotEqualToFilter<Integer> filter = new NotEqualToFilter<>(1);

        writer.writeValue(serialized, filter);
        assertEquals("[\"neq\",1]", serialized.toString());
    }

    @Test
    public void testEqInt() throws IOException
    {
        EqualToFilter<Integer> filter = new EqualToFilter<>(1);

        writer.writeValue(serialized, filter);
        assertEquals("[\"eq\",1]", serialized.toString());
    }

    @Test
    public void testSetMemberInt() throws Exception
    {
        SetMemberFilter<Integer> filter = new SetMemberFilter<>(1, 2, 3);

        writer.writeValue(serialized, filter);
        assertEquals("[\"set_member\",1,2,3]", serialized.toString());
    }

    @Test
    public void testSimilarTo() throws IOException
    {
        SimilarToFilter filter =
            new SimilarToFilter("string", 2);

        writer.writeValue(serialized, filter);
        assertEquals("[\"similar_to\",\"string\",2]", serialized.toString());
    }

    @Test
    public void testStartsWith() throws IOException
    {
        StartsWithFilter filter =
            new StartsWithFilter("string");

        writer.writeValue(serialized, filter);
        assertEquals("[\"starts_with\",\"string\"]", serialized.toString());
    }

    @Test
    public void testEndsWith() throws IOException
    {
        EndsWithFilter filter =
            new EndsWithFilter("string");

        writer.writeValue(serialized, filter);
        assertEquals("[\"ends_with\",\"string\"]", serialized.toString());
    }

    @Test
    public void testAnd() throws IOException
    {

        LogicalAndFilter filter =
            new LogicalAndFilter(new EmptyFilter(), new EmptyFilter());

        writer.writeValue(serialized, filter);
        assertEquals("[\"and\",[[\"empty\"]],[[\"empty\"]]]", serialized.toString());
    }

    @Test
    public void testOr() throws IOException
    {

        LogicalOrFilter filter =
            new LogicalOrFilter(new EmptyFilter(), new EmptyFilter());

        writer.writeValue(serialized, filter);
        assertEquals("[\"or\",[[\"empty\"]],[[\"empty\"]]]", serialized.toString());
    }

    @Test
    public void testNot() throws IOException
    {

        LogicalNotFilter filter =
            new LogicalNotFilter(new EmptyFilter(), new EmptyFilter());

        writer.writeValue(serialized, filter);
        assertEquals("[\"not\",[[\"empty\"]],[[\"empty\"]]]", serialized.toString());
    }

    @Test
    public void testIntToString() throws Exception
    {
        IntToStringFilter filter = new IntToStringFilter();

        writer.writeValue(serialized, filter);
        assertEquals("[\"int_to_string\"]", serialized.toString());
    }

    @Test
    public void testStringToInt() throws Exception
    {
        StringToIntFilter filter = new StringToIntFilter();

        writer.writeValue(serialized, filter);
        assertEquals("[\"string_to_int\"]", serialized.toString());
    }

    @Test
    public void testFloatToString() throws Exception
    {
        FloatToStringFilter filter = new FloatToStringFilter();

        writer.writeValue(serialized, filter);
        assertEquals("[\"float_to_string\"]", serialized.toString());
    }

    @Test
    public void testStringToFloat() throws Exception
    {
        StringToFloatFilter filter = new StringToFloatFilter();

        writer.writeValue(serialized, filter);
        assertEquals("[\"string_to_float\"]", serialized.toString());
    }

    @Test
    public void testToUpper() throws Exception
    {
        ToUpperFilter filter = new ToUpperFilter();

        writer.writeValue(serialized, filter);
        assertEquals("[\"to_upper\"]", serialized.toString());
    }

    @Test
    public void testToLower() throws Exception
    {
        ToLowerFilter filter = new ToLowerFilter();

        writer.writeValue(serialized, filter);
        assertEquals("[\"to_lower\"]", serialized.toString());
    }

    @Test
    public void testTokenize() throws Exception
    {
        TokenizeFilter filter = new TokenizeFilter("-", 2);

        writer.writeValue(serialized, filter);
        assertEquals("[\"tokenize\",\"-\",2]", serialized.toString());
    }

    @Test
    public void testUrlDecode() throws Exception
    {
        UrlDecodeFilter filter = new UrlDecodeFilter();

        writer.writeValue(serialized, filter);
        assertEquals("[\"urldecode\"]", serialized.toString());
    }

    private static class EmptyFilter extends KeyFilter
    {

        protected EmptyFilter()
        {
            super("empty");
        }

    }

}
