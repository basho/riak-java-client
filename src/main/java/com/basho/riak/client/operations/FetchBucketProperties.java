package com.basho.riak.client.operations;

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.FetchBucketPropsOperation;
import com.basho.riak.client.query.Location;

import java.util.concurrent.ExecutionException;

public class FetchBucketProperties extends RiakCommand<FetchBucketPropsOperation.Response>
{

	private final Location location;

	public FetchBucketProperties(Builder builder)
	{
		this.location = builder.location;
	}

	@Override
	FetchBucketPropsOperation.Response execute(RiakCluster cluster) throws ExecutionException, InterruptedException
	{
		FetchBucketPropsOperation.Builder operation = 
            new FetchBucketPropsOperation.Builder(location);
		
        return cluster.execute(operation.build()).get();
	}

	public static class Builder
	{

		private final Location location;

		public Builder(Location location)
		{
			if (location == null)
            {
                throw new IllegalArgumentException("Location cannot be null");
            }
            this.location = location;
		}

		public FetchBucketProperties build()
		{
			return new FetchBucketProperties(this);
		}
	}

}
