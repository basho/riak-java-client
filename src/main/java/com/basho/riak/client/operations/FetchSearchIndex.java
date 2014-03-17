package com.basho.riak.client.operations;

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.YzFetchIndexOperation;

import java.util.concurrent.ExecutionException;

public class FetchSearchIndex extends RiakCommand<YzFetchIndexOperation.Response>
{
	private final String index;

	FetchSearchIndex(Builder builder)
	{
		this.index = builder.index;
	}

	@Override
	YzFetchIndexOperation.Response execute(RiakCluster cluster) throws ExecutionException, InterruptedException
	{
	    YzFetchIndexOperation.Builder builder = new YzFetchIndexOperation.Builder();
	    builder.withIndexName(index);
	    YzFetchIndexOperation operation = builder.build();
	    return cluster.execute(operation).get();
	}

	public static class Builder
	{
		private final String index;

		public Builder(String index)
		{
			this.index = index;
		}

		public FetchSearchIndex build()
		{
			return new FetchSearchIndex(this);
		}
	}
}
