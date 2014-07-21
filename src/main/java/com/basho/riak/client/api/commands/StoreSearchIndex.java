package com.basho.riak.client.api.commands;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.YzPutIndexOperation;
import com.basho.riak.client.core.query.search.YokozunaIndex;

import java.util.concurrent.ExecutionException;

 /*
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class StoreSearchIndex extends RiakCommand<Void, YokozunaIndex>
{
	private final YokozunaIndex index;

	StoreSearchIndex(Builder builder)
	{
		this.index = builder.index;
	}

	@Override
    protected RiakFuture<Void, YokozunaIndex> executeAsync(RiakCluster cluster)
    {
        RiakFuture<Void, YokozunaIndex> coreFuture =
            cluster.execute(buildCoreOperation());
        
        CoreFutureAdapter<Void, YokozunaIndex, Void, YokozunaIndex> future =
            new CoreFutureAdapter<Void, YokozunaIndex, Void, YokozunaIndex>(coreFuture)
            {
                @Override
                protected Void convertResponse(Void coreResponse)
                {
                    return coreResponse;
                }

                @Override
                protected YokozunaIndex convertQueryInfo(YokozunaIndex coreQueryInfo)
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
