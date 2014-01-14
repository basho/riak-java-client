package com.basho.riak.client.operations;

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.YzGetSchemaOperation;
import com.basho.riak.client.query.search.YokozunaSchema;

import java.util.concurrent.ExecutionException;

public class FetchSchema extends RiakCommand<YokozunaSchema>
{
	private final String schema;

	FetchSchema(Builder builder)
	{
		this.schema = builder.schema;
	}

	@Override
	YokozunaSchema execute(RiakCluster cluster) throws ExecutionException, InterruptedException
	{
		YzGetSchemaOperation operation = new YzGetSchemaOperation.Builder(schema).build();
		return cluster.execute(operation).get();
	}

	public static class Builder
	{
		private final String schema;

		public Builder(String schema)
		{
			this.schema = schema;
		}

		public FetchSchema build()
		{
			return new FetchSchema(this);
		}
	}
}
