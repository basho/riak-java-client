package com.basho.riak.client.operations;

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.YzPutIndexOperation;
import com.basho.riak.client.query.search.YokozunaIndex;

import java.util.concurrent.ExecutionException;

public class StoreSearchIndex extends RiakCommand<YzPutIndexOperation.Response>
{
	private final YokozunaIndex index;

	StoreSearchIndex(Builder builder)
	{
		this.index = builder.index;
	}

	@Override
	public YzPutIndexOperation.Response execute(RiakCluster cluster) throws ExecutionException, InterruptedException
	{
	    YzPutIndexOperation operation = new YzPutIndexOperation.Builder(index).build();
	    return cluster.execute(operation).get();
	}

	public static class Builder
	{
		private final YokozunaIndex index;

		public Builder(YokozunaIndex index)
		{
			this.index = index;
		}

		public StoreSearchIndex build()
		{
			return new StoreSearchIndex(this);
		}
	}
}
