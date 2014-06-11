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
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.ListBucketsOperation;
import com.basho.riak.client.query.Namespace;
import com.basho.riak.client.util.BinaryValue;

import java.util.Iterator;
import java.util.List;

 /*
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class ListBuckets extends RiakCommand<ListBuckets.Response, BinaryValue>
{
    private final int timeout;
    private final BinaryValue type;

    ListBuckets(Builder builder)
    {
		this.timeout = builder.timeout;
	    this.type = builder.type;
    }

    @Override
    protected RiakFuture<Response, BinaryValue> executeAsync(RiakCluster cluster)
    {
        RiakFuture<ListBucketsOperation.Response, BinaryValue> coreFuture =
            cluster.execute(buildCoreOperation());
        
        CoreFutureAdapter<ListBuckets.Response, BinaryValue, ListBucketsOperation.Response, BinaryValue> future =
            new CoreFutureAdapter<ListBuckets.Response, BinaryValue, ListBucketsOperation.Response, BinaryValue>(coreFuture)
            {
                @Override
                protected Response convertResponse(ListBucketsOperation.Response coreResponse)
                {
                    return new Response(type, coreResponse.getBuckets());
                }

                @Override
                protected BinaryValue convertQueryInfo(BinaryValue coreQueryInfo)
                {
                    return coreQueryInfo;
                }
            };
        coreFuture.addListener(future);
        return future;
    }

    private ListBucketsOperation buildCoreOperation()
    {
        ListBucketsOperation.Builder builder = new ListBucketsOperation.Builder();
        if (timeout > 0)
        {
            builder.withTimeout(timeout);
        }
        return builder.build();
    }

    public static class Response implements Iterable<Namespace> {

        private final BinaryValue type;
        private final List<BinaryValue> buckets;

        public Response(BinaryValue type, List<BinaryValue> buckets)
        {
            this.type = type;
            this.buckets = buckets;
        }

        @Override
        public Iterator<Namespace> iterator()
        {
            return new Itr(buckets.iterator(), type);
        }
    }

    private static class Itr implements Iterator<Namespace>
    {
        private final Iterator<BinaryValue> iterator;
        private final BinaryValue type;

        private Itr(Iterator<BinaryValue> iterator, BinaryValue type)
        {
            this.iterator = iterator;
            this.type = type;
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        @Override
        public Namespace next()
        {
            BinaryValue bucket = iterator.next();
            return new Namespace(type, bucket);
        }

        @Override
        public void remove()
        {
            iterator.remove();
        }
    }

	public static class Builder
	{
		private int timeout;
		private final BinaryValue type;

		public Builder(String type)
		{
			this.type = BinaryValue.create(type);
		}

		public Builder(BinaryValue type)
		{
			this.type = type;
		}

		public Builder withTimeout(int timeout)
		{
			this.timeout = timeout;
			return this;
		}

		public ListBuckets build()
		{
			return new ListBuckets(this);
		}
	}

}
