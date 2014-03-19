package com.basho.riak.client.query.filters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class Test
{
	public static void main(String... args) throws IOException
	{

		ObjectMapper objectMapper = new ObjectMapper();
		ObjectWriter objectWriter = objectMapper.writer();
		List<KeyFilter> filters = new LinkedList<KeyFilter>();
		filters.add(new BetweenFilter<Integer>(1, 2));
		filters.add(new BetweenFilter<Double>(1.0, 2.0));
		filters.add(new BetweenFilter<String>("a", "b"));
		filters.add(new EndsWithFilter("dave"));
		filters.add(new FloatToStringFilter());
		filters.add(new GreaterThanFilter<Integer>(1));
		filters.add(new GreaterThanFilter<Double>(1.0));
		filters.add(new GreaterThanFilter<String>("a"));
		filters.add(new GreaterThanOrEqualFilter<Integer>(1));
		filters.add(new GreaterThanOrEqualFilter<Double>(1.0));
		filters.add(new GreaterThanOrEqualFilter<String>("a"));
		filters.add(new IntToStringFilter());
		filters.add(new LessThanFilter<Integer>(1));
		filters.add(new LessThanFilter<Double>(1.0));
		filters.add(new LessThanFilter<String>("a"));
		filters.add(new LessThanOrEqualFilter<Integer>(1));
		filters.add(new LessThanOrEqualFilter<Double>(1.0));
		filters.add(new LessThanOrEqualFilter<String>("a"));
		filters.add(new MatchFilter("a"));
		filters.add(new NotEqualToFilter<Integer>(1));
		filters.add(new NotEqualToFilter<Double>(1.0));
		filters.add(new NotEqualToFilter<String>("a"));
		filters.add(new SetMemberFilter<Integer>(1, 2, 3));
		filters.add(new SetMemberFilter<Double>(1.0, 2.0, 3.0));
		filters.add(new SetMemberFilter<String>("a", "b", "c"));
		filters.add(new StringToIntFilter());
		filters.add(new TokenizeFilter("-", 10));
		filters.add(new ToLowerFilter());
		filters.add(new ToUpperFilter());
		filters.add(new UrlDecodeFilter());
		filters.add(new LogicalAndFilter(new BetweenFilter<Integer>(1, 2), new BetweenFilter<String>("a", "b")));
		filters.add(new LogicalNotFilter(new BetweenFilter<Integer>(1, 2), new BetweenFilter<String>("a", "b")));
		filters.add(new LogicalOrFilter(new BetweenFilter<Integer>(1, 2), new BetweenFilter<String>("a", "b")));
		filters.add(new LogicalOrFilter(new BetweenFilter<Integer>(1, 2), new BetweenFilter<String>("a", "b")));
		objectWriter.writeValue(System.out, filters);

	}
}
