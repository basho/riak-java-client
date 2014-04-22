package com.basho.riak.client.operations.mapreduce;

class SearchInput implements MapReduceInput
{
	private final String index;
	private final String search;

	public SearchInput(String index, String search)
	{
		this.index = index;
		this.search = search;
	}

	public String getIndex()
	{
		return index;
	}

	public String getSearch()
	{
		return search;
	}

}
