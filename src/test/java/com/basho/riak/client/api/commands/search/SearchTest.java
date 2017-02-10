package com.basho.riak.client.api.commands.search;

import org.junit.Test;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class SearchTest
{
	@Test
	public void equalsReturnsTrueForEqualNamespaces()
	{
		Search search1 = new Search.Builder("index", "query").filter("filter")
				.withPresort(Search.Presort.KEY).sort("sort")
				.returnFields("field1", "field2")
				.withStart(10).withRows(10)
				.withOption(Search.Option.DEFAULT_OPERATION, Search.Option.Operation.AND).build();
		Search search2 = new Search.Builder("index", "query").filter("filter")
				.withPresort(Search.Presort.KEY).sort("sort")
				.returnFields("field1", "field2")
				.withStart(10).withRows(10)
				.withOption(Search.Option.DEFAULT_OPERATION, Search.Option.Operation.AND).build();

		assertThat(search1, is(equalTo(search2)));
		assertThat(search2, is(equalTo(search1)));
	}

	@Test
	public void equalsReturnsFalseForDifferentNamespaces()
	{
		Search search1 = new Search.Builder("index1", "query1").filter("filter1")
				.withPresort(Search.Presort.KEY).sort("sort1")
				.returnFields("field1")
				.withStart(10).withRows(10)
				.withOption(Search.Option.DEFAULT_OPERATION, Search.Option.Operation.AND).build();
		Search search2 = new Search.Builder("index2", "query2").filter("filter2")
				.withPresort(Search.Presort.SCORE).sort("sort2")
				.returnFields("field2")
				.withStart(5).withRows(5)
				.withOption(Search.Option.DEFAULT_FIELD, "field2").build();

		assertThat(search1, is(not(equalTo(search2))));
		assertThat(search2, is(not(equalTo(search1))));
	}
}