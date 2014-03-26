package com.basho.riak.client.operations;

import com.basho.riak.client.RiakCommand;
import com.basho.riak.client.core.FailureInfo;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.YzDeleteIndexOperation;

import java.util.concurrent.ExecutionException;

/**
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class DeleteSearchIndex extends RiakCommand<YzDeleteIndexOperation.Response, String>
{
	private final String index;

	DeleteSearchIndex(Builder builder)
	{
		this.index = builder.index;
	}

	@Override
	protected final YzDeleteIndexOperation.Response doExecute(RiakCluster cluster) throws ExecutionException, InterruptedException
	{
		RiakFuture<YzDeleteIndexOperation.Response, String> future = doExecuteAsync(cluster);
        
        future.await();
        if (future.isSuccess())
        {
            return future.get();
        }
        else
        {
            throw new ExecutionException(future.cause().getCause());
        }
	}

    @Override
    protected final RiakFuture<YzDeleteIndexOperation.Response, String> doExecuteAsync(RiakCluster cluster)
    {
        RiakFuture<YzDeleteIndexOperation.Response, String> coreFuture =
            cluster.execute(buildCoreOperation());
        
        CoreFutureAdapter<YzDeleteIndexOperation.Response, String, YzDeleteIndexOperation.Response, String> future =
            new CoreFutureAdapter<YzDeleteIndexOperation.Response, String, YzDeleteIndexOperation.Response, String>(coreFuture)
            {

                @Override
                protected YzDeleteIndexOperation.Response convertResponse(YzDeleteIndexOperation.Response coreResponse)
                {
                    return coreResponse;
                }

                @Override
                protected FailureInfo<String> convertFailureInfo(FailureInfo<String> coreQueryInfo)
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
