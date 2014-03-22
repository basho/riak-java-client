package com.basho.riak.client.operations;

import com.basho.riak.client.RiakCommand;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.YzDeleteIndexOperation;

import java.util.concurrent.ExecutionException;

/**
 * @author Dave Rusek <drusuk at basho dot com>
 * @since 2.0
 */
public final class DeleteSearchIndex extends RiakCommand<YzDeleteIndexOperation.Response>
{
	private final String index;

	DeleteSearchIndex(Builder builder)
	{
		this.index = builder.index;
	}

	@Override
	protected final YzDeleteIndexOperation.Response doExecute(RiakCluster cluster) throws ExecutionException, InterruptedException
	{
		YzDeleteIndexOperation operation = new YzDeleteIndexOperation.Builder(index).build();
		return cluster.execute(operation).get();
	}

	public static class Builder
	{

		private final String index;

		public Builder(String index)
		{
			this.index = index;
		}

		public DeleteSearchIndex build()
		{
			return new DeleteSearchIndex(this);
		}
	}
}
