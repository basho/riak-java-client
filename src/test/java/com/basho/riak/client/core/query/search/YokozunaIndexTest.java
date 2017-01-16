package com.basho.riak.client.core.query.search;

import org.junit.Test;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class YokozunaIndexTest {
	@Test
	public void equalsReturnsTrueForEqualYokozunaIndices() {
		YokozunaIndex yokozunaIndex1 = new YokozunaIndex("name", "schema").withNVal(3);
		YokozunaIndex yokozunaIndex2 = new YokozunaIndex("name", "schema").withNVal(3);

		assertThat(yokozunaIndex1, is(equalTo(yokozunaIndex2)));
		assertThat(yokozunaIndex2, is(equalTo(yokozunaIndex1)));
	}

	@Test
	public void equalsReturnsFalseForDifferentYokozunaIndices() {
		YokozunaIndex yokozunaIndex1 = new YokozunaIndex("name1", "schema1").withNVal(3);
		YokozunaIndex yokozunaIndex2 = new YokozunaIndex("name2", "schema2").withNVal(5);

		assertThat(yokozunaIndex1, is(not(equalTo(yokozunaIndex2))));
		assertThat(yokozunaIndex2, is(not(equalTo(yokozunaIndex1))));
	}
}