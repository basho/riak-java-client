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
package com.basho.riak.client.api.commands.buckets;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.api.commands.CoreFutureAdapter;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.ListBucketsOperation;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;

import java.util.Iterator;
import java.util.List;

/**
 * Command used to list the buckets contained in a bucket type.
 * <script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>
 * <p>
 * <pre class="prettyprint">
 * {@code
 * ListBuckets lb = new ListBuckets.Builder("my_type").build();
 * ListBuckets.Response resp = client.execute(lb);
 * for (Namespace ns : response)
 * {
 *     System.out.println(ns.getBucketName());
 * }}</pre>
 * </p>
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

        if (type != null)
        {
            builder.withBucketType(type);
        }

        return builder.build();
    }

    /**
     * A response from a ListBuckets command.
     * <p>This encapsulates an immutable list of bucket names 
     * and is Iterable:
     * <pre>
     * {@code
     * for (Namespace ns : response)
     * {
     *     System.out.println(ns.getBucketName());
     * }
     * }
     * </pre>
     * </p>
     */
    public static class Response implements Iterable<Namespace>
    {

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

    /**
     * Builder for a ListBuckets command.
     */
	public static class Builder
	{
		private int timeout;
		private final BinaryValue type;

        /**
         * Construct a Builder for a ListBuckets command.
         * @param type the bucket type.
         */
		public Builder(String type)
		{
			this.type = BinaryValue.create(type);
		}

        /**
         * Construct a Builder for a ListBuckets command.
         * @param type the bucket type.
         */
		public Builder(BinaryValue type)
		{
			this.type = type;
		}

        /**
         * Set the Riak-side timeout value.
         * <p>
         * By default, riak has a 60s timeout for operations. Setting
         * this value will override that default for this operation.
         * </p>
         * @param timeout the timeout in milliseconds to be sent to riak.
         * @return a reference to this object.
         */
		public Builder withTimeout(int timeout)
		{
			this.timeout = timeout;
			return this;
		}

        /**
         * Construct a new ListBuckets command.
         * @return a new ListBuckets command.
         */
		public ListBuckets build()
		{
			return new ListBuckets(this);
		}
	}

}
