package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.filters.KeyFilter;

import java.util.Collection;

class BucketInput implements MapReduceInput
{

	private final Location bucket;
	private final Collection<KeyFilter> filters;

	public BucketInput(Location bucket, Collection<KeyFilter> filters)
	{
		this.bucket = bucket;
		this.filters = filters;
	}

	public Location getBucket()
	{
		return bucket;
	}

	public Collection<KeyFilter> getFilters()
	{
		return filters;
	}

}