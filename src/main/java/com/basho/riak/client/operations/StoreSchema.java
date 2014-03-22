package com.basho.riak.client.operations;

import com.basho.riak.client.RiakCommand;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.YzPutSchemaOperation;
import com.basho.riak.client.query.search.YokozunaSchema;

import java.util.concurrent.ExecutionException;

 /*
 * @author Dave Rusek <drusuk at basho dot com>
 * @since 2.0
 */
public final class StoreSchema extends RiakCommand<YzPutSchemaOperation.Response>
{
	private final YokozunaSchema schema;

	StoreSchema(Builder builder)
	{
		this.schema = builder.schema;
	}

	@Override
	protected final YzPutSchemaOperation.Response doExecute(RiakCluster cluster) throws ExecutionException, InterruptedException
	{
	    YzPutSchemaOperation operation = new YzPutSchemaOperation.Builder(schema).build();
	    return cluster.execute(operation).get();
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
