package com.basho.riak.client.operations;

import com.basho.riak.client.RiakCommand;
import com.basho.riak.client.core.FailureInfo;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.YzPutIndexOperation;
import com.basho.riak.client.query.search.YokozunaIndex;

import java.util.concurrent.ExecutionException;

 /*
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class StoreSearchIndex extends RiakCommand<YzPutIndexOperation.Response, YokozunaIndex>
{
	private final YokozunaIndex index;

	StoreSearchIndex(Builder builder)
	{
		this.index = builder.index;
	}

	@Override
	protected final YzPutIndexOperation.Response doExecute(RiakCluster cluster) throws ExecutionException, InterruptedException
	{
	    RiakFuture<YzPutIndexOperation.Response, YokozunaIndex> future =
            doExecuteAsync(cluster);
	    
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
    protected RiakFuture<YzPutIndexOperation.Response, YokozunaIndex> doExecuteAsync(RiakCluster cluster)
    {
        RiakFuture<YzPutIndexOperation.Response, YokozunaIndex> coreFuture =
            cluster.execute(buildCoreOperation());
        
        CoreFutureAdapter<YzPutIndexOperation.Response, YokozunaIndex, YzPutIndexOperation.Response, YokozunaIndex> future =
            new CoreFutureAdapter<YzPutIndexOperation.Response, YokozunaIndex, YzPutIndexOperation.Response, YokozunaIndex>(coreFuture)
            {
                @Override
                protected YzPutIndexOperation.Response convertResponse(YzPutIndexOperation.Response coreResponse)
                {
                    return coreResponse;
                }

                @Override
                protected FailureInfo<YokozunaIndex> convertFailureInfo(FailureInfo<YokozunaIndex> coreQueryInfo)
                {
                    return coreQueryInfo;
                }
            };
        coreFuture.addListener(future);
        return future;
    }
    
    private final YzPutIndexOperation buildCoreOperation()
    {
        return new YzPutIndexOperation.Builder(index).build();
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
