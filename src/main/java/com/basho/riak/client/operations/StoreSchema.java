package com.basho.riak.client.operations;

import com.basho.riak.client.RiakCommand;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.YzPutSchemaOperation;
import com.basho.riak.client.query.search.YokozunaSchema;

import java.util.concurrent.ExecutionException;

 /*
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class StoreSchema extends RiakCommand<Void, YokozunaSchema>
{
	private final YokozunaSchema schema;

	StoreSchema(Builder builder)
	{
		this.schema = builder.schema;
	}

	@Override
    protected RiakFuture<Void, YokozunaSchema> executeAsync(RiakCluster cluster)
    {
        RiakFuture<Void, YokozunaSchema> coreFuture =
            cluster.execute(buildCoreOperation());
        
        CoreFutureAdapter<Void, YokozunaSchema,Void, YokozunaSchema> future =
            new CoreFutureAdapter<Void, YokozunaSchema,Void, YokozunaSchema>(coreFuture)
            {
                @Override
                protected Void convertResponse(Void coreResponse)
                {
                    return coreResponse;
                }

                @Override
                protected YokozunaSchema convertQueryInfo(YokozunaSchema coreQueryInfo)
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
