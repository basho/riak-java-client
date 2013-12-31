package com.basho.riak.client.operations;

import com.basho.riak.client.core.RiakCluster;

import java.util.concurrent.ExecutionException;

public class StoreBucketProperties extends RiakCommand<Boolean>
{

	@Override
	Boolean execute(RiakCluster cluster) throws ExecutionException, InterruptedException
	{
		return null;
	}

}
