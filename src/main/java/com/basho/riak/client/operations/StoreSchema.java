package com.basho.riak.client.operations;

import com.basho.riak.client.RiakCommand;
import com.basho.riak.client.core.FailureInfo;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.YzPutSchemaOperation;
import com.basho.riak.client.query.search.YokozunaSchema;

import java.util.concurrent.ExecutionException;

 /*
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class StoreSchema extends RiakCommand<YzPutSchemaOperation.Response, YokozunaSchema>
{
	private final YokozunaSchema schema;

	StoreSchema(Builder builder)
	{
		this.schema = builder.schema;
	}

	@Override
	protected final YzPutSchemaOperation.Response doExecute(RiakCluster cluster) throws ExecutionException, InterruptedException
	{
	    RiakFuture<YzPutSchemaOperation.Response, YokozunaSchema> future =
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
    protected RiakFuture<YzPutSchemaOperation.Response, YokozunaSchema> doExecuteAsync(RiakCluster cluster)
    {
        RiakFuture<YzPutSchemaOperation.Response, YokozunaSchema> coreFuture =
            cluster.execute(buildCoreOperation());
        
        CoreFutureAdapter<YzPutSchemaOperation.Response, YokozunaSchema,YzPutSchemaOperation.Response, YokozunaSchema> future =
            new CoreFutureAdapter<YzPutSchemaOperation.Response, YokozunaSchema,YzPutSchemaOperation.Response, YokozunaSchema>(coreFuture)
            {
                @Override
                protected YzPutSchemaOperation.Response convertResponse(YzPutSchemaOperation.Response coreResponse)
                {
                    return coreResponse;
                }

                @Override
                protected FailureInfo<YokozunaSchema> convertFailureInfo(FailureInfo<YokozunaSchema> coreQueryInfo)
                {
                    return coreQueryInfo;
                }
            };
        coreFuture.addListener(future);
        return future;
    }

    private YzPutSchemaOperation buildCoreOperation()
    {
        return new YzPutSchemaOperation.Builder(schema).build();
    }

    public static class Builder
	{
		private final YokozunaSchema schema;

		public Builder(YokozunaSchema schema)
		{
			this.schema = schema;
		}

		public StoreSchema build()
		{
			return new StoreSchema(this);
		}
	}
}
