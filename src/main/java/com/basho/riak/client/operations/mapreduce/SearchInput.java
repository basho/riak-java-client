package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.query.Location;

public class SearchInput implements MapReduceInput
{
	private final Location bucket;
	private final String search;

	public SearchInput(Location bucket, String search)
	{
		this.bucket = bucket;
		this.search = search;
	}

	public Location getBucket()
	{
		return bucket;
	}

	public String getSearch()
	{
		return search;
	}

}
