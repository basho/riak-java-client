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

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.ListKeysOperation;
import com.basho.riak.client.util.BinaryValue;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ListKeys extends RiakCommand<ListKeys.Response>
{

	private final Location bucket;
	private final int timeout;

	ListKeys(Builder builder)
	{
		this.bucket = builder.bucket;
		this.timeout = builder.timeout;
	}

	@Override
	public Response execute(RiakCluster cluster) throws ExecutionException, InterruptedException
	{
		ListKeysOperation.Builder builder = new ListKeysOperation.Builder(bucket.getBucket());

		if (timeout > 0)
		{
			builder.withTimeout(timeout);
		}

		if (bucket.hasType())
		{
			builder.withBucketType(bucket.getType());
		}

		ListKeysOperation operation = builder.build();
		cluster.execute(operation);

		return new Response(bucket.getBucket(), operation.get());
	}

	public static class Response implements Iterable<Location>
	{

		private final BinaryValue bucket;
		private final List<BinaryValue> keys;

		public Response(BinaryValue bucket, List<BinaryValue> keys)
		{
			this.bucket = bucket;
			this.keys = keys;
		}

		@Override
		public Iterator<Location> iterator()
		{
			return new Itr(bucket, keys.iterator());
		}
	}

	private static class Itr implements Iterator<Location>
	{
		private final Iterator<BinaryValue> iterator;
		private final BinaryValue bucket;

		private Itr(BinaryValue bucket, Iterator<BinaryValue> iterator)
		{
			this.iterator = iterator;
			this.bucket = bucket;
		}

		@Override
		public boolean hasNext()
		{
			return iterator.hasNext();
		}

		@Override
		public Location next()
		{
			BinaryValue key = iterator.next();
			return new Location(bucket, key);
		}

		@Override
		public void remove()
		{
			iterator.remove();
		}
	}

	public static class Builder
	{
		private final Location bucket;
		private int timeout;

		public Builder(Location bucket)
		{
			this.bucket = bucket;
		}

		public Builder withTimeout(int timeout)
		{
			this.timeout = timeout;
			return this;
		}

		public ListKeys build()
		{
			return new ListKeys(this);
		}
	}

}
