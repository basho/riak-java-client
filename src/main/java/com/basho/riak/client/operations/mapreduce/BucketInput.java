package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.filters.KeyFilter;

import java.util.Collection;

public class BucketInput implements MapReduceInput
{

	private final Location location;
	private final Collection<KeyFilter> filters;

	public BucketInput(Location location, Collection<KeyFilter> filters)
	{
		this.location = location;
		this.filters = filters;
	}

	public Location getLocation()
	{
		return location;
	}

    public boolean hasFilters()
    {
        return filters != null && !filters.isEmpty();
    }
    
	public Collection<KeyFilter> getFilters()
	{
		return filters;
	}

}