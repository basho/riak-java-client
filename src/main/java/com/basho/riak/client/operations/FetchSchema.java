/*
 * Copyright 2013 Basho Technologies Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.basho.riak.client.operations;

import com.basho.riak.client.RiakCommand;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.YzGetSchemaOperation;

import java.util.concurrent.ExecutionException;

 /*
 * @author Dave Rusek <drusuk at basho dot com>
 * @since 2.0
 */
public final class FetchSchema extends RiakCommand<YzGetSchemaOperation.Response>
{
	private final String schema;

	FetchSchema(Builder builder)
	{
		this.schema = builder.schema;
	}

	@Override
	protected YzGetSchemaOperation.Response doExecute(RiakCluster cluster) throws ExecutionException, InterruptedException
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
