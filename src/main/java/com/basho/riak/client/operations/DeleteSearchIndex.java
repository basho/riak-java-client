package com.basho.riak.client.operations;

import com.basho.riak.client.RiakCommand;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.YzDeleteIndexOperation;

import java.util.concurrent.ExecutionException;

/**
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class DeleteSearchIndex extends RiakCommand<Void, String>
{
	private final String index;

	DeleteSearchIndex(Builder builder)
	{
		this.index = builder.index;
	}

	@Override
    protected final RiakFuture<Void, String> executeAsync(RiakCluster cluster)
    {
        RiakFuture<Void, String> coreFuture =
            cluster.execute(buildCoreOperation());
        
        CoreFutureAdapter<Void, String, Void, String> future =
            new CoreFutureAdapter<Void, String, Void, String>(coreFuture)
            {

                @Override
                protected Void convertResponse(Void coreResponse)
                {
                    return coreResponse;
                }

                @Override
                protected String convertQueryInfo(String coreQueryInfo)
                {
                    return coreQueryInfo;
                }
            };
        coreFuture.addListener(future);
        return future;
    }

    private YzDeleteIndexOperation buildCoreOperation()
    {
        return new YzDeleteIndexOperation.Builder(index).build();
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
