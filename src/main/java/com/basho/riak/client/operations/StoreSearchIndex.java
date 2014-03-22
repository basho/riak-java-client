package com.basho.riak.client.operations;

import com.basho.riak.client.RiakCommand;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.YzPutIndexOperation;
import com.basho.riak.client.query.search.YokozunaIndex;

import java.util.concurrent.ExecutionException;

 /*
 * @author Dave Rusek <drusuk at basho dot com>
 * @since 2.0
 */
public final class StoreSearchIndex extends RiakCommand<YzPutIndexOperation.Response>
{
	private final YokozunaIndex index;

	StoreSearchIndex(Builder builder)
	{
		this.index = builder.index;
	}

	@Override
	protected final YzPutIndexOperation.Response doExecute(RiakCluster cluster) throws ExecutionException, InterruptedException
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
