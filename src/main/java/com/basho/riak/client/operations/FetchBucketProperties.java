package com.basho.riak.client.operations;

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.FetchBucketPropsOperation;
import com.basho.riak.client.query.BucketProperties;

import java.util.concurrent.ExecutionException;

public class FetchBucketProperties extends RiakCommand<BucketProperties>
{

	private final Location location;

	public FetchBucketProperties(Builder builder)
	{
		this.location = builder.location;
	}

	@Override
	public BucketProperties execute(RiakCluster cluster) throws ExecutionException, InterruptedException
	{
		FetchBucketPropsOperation.Builder operation = new FetchBucketPropsOperation.Builder(location.getBucket());

		if (location.hasType())
		{
			operation.withBucketType(location.getType());
		}

		return cluster.execute(operation.build()).get();
	}

	public static class Builder
	{

		private final Location location;

		public Builder(Location location)
		{
			this.location = location;
		}

		public FetchBucketProperties build()
		{
			return new FetchBucketProperties(this);
		}
	}

}
