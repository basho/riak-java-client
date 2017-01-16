package com.basho.riak.client.api.commands.search;

import com.basho.riak.client.core.query.search.YokozunaIndex;
import org.junit.Test;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class StoreIndexTest {
	@Test
	public void equalsReturnsTrueForEqualIndexAndTimeout() {
		YokozunaIndex index1 = new YokozunaIndex("index");
		YokozunaIndex index2 = new YokozunaIndex("index");

		StoreIndex storeIndex1 = new StoreIndex.Builder(index1).withTimeout(5).build();
		StoreIndex storeIndex2 = new StoreIndex.Builder(index2).withTimeout(5).build();

		assertThat(storeIndex1, is(equalTo(storeIndex2)));
		assertThat(storeIndex2, is(equalTo(storeIndex1)));
	}

	@Test
	public void equalsReturnsFalseForDifferentIndexAndTimeout() {
		YokozunaIndex index1 = new YokozunaIndex("index1");
		YokozunaIndex index2 = new YokozunaIndex("index2");

		StoreIndex storeIndex1 = new StoreIndex.Builder(index1).withTimeout(5).build();
		StoreIndex storeIndex2 = new StoreIndex.Builder(index2).withTimeout(8).build();

		assertThat(storeIndex1, is(not(equalTo(storeIndex2))));
		assertThat(storeIndex2, is(not(equalTo(storeIndex1))));
	}
}