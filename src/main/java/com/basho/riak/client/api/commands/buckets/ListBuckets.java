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

import com.basho.riak.client.api.commands.ChunkedResponseIterator;
import com.basho.riak.client.api.ListException;
import com.basho.riak.client.api.StreamableRiakCommand;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.StreamingRiakFuture;
import com.basho.riak.client.core.operations.ListBucketsOperation;
import com.basho.riak.client.core.query.ConvertibleIterator;
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
 * <p>
 * You can also stream the results back before the operation is fully complete.
 * This reduces the time between executing the operation and seeing a result,
 * and reduces overall memory usage if the iterator is consumed quickly enough.
 * The result iterable can only be iterated once though.
 * If the thread is interrupted while the iterator is polling for more results,
 * a {@link RuntimeException} will be thrown.
 * <pre class="prettyprint">
 * {@code
 * ListBuckets lb = new ListBuckets.Builder("my_type").build();
 * final RiakFuture<ListBuckets.StreamingResponse, BinaryValue> streamFuture =
 *     client.executeAsyncStreaming(lb, 200);
 * final ListBuckets.StreamingResponse streamingResponse = streamFuture.get();
 * for (Namespace ns : streamingResponse)
 * {
 *     System.out.println(ns.getBucketName());
 * }}</pre>
 * </p>
 * @author Dave Rusek <drusek at basho dot com>
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0
 */
public final class ListBuckets extends StreamableRiakCommand.StreamableRiakCommandWithSameInfo<ListBuckets.Response, BinaryValue,
        ListBucketsOperation.Response>
{
    private final int timeout;
    private final BinaryValue type;

    ListBuckets(Builder builder) throws ListException
    {
        this.timeout = builder.timeout;
        this.type = builder.type;

        if (!builder.allowListing)
        {
            throw new ListException();
        }
    }

    @Override
    protected Response convertResponse(FutureOperation<ListBucketsOperation.Response, ?, BinaryValue> request,
                                       ListBucketsOperation.Response coreResponse)
    {
        return new Response(type, coreResponse.getBuckets());
    }

    @Override
    protected Response createResponse(int timeout, StreamingRiakFuture<ListBucketsOperation.Response, BinaryValue> coreFuture)
    {
        return new Response(type, timeout, coreFuture);
    }

    @Override
    protected ListBucketsOperation buildCoreOperation(boolean streamResults)
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

        builder.streamResults(streamResults);

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
    public static class Response extends StreamableRiakCommand.StreamableResponse<Namespace, BinaryValue>
    {
        private final BinaryValue type;
        private final List<BinaryValue> buckets;

        Response(BinaryValue type,
                          int pollTimeout,
                          StreamingRiakFuture<ListBucketsOperation.Response, BinaryValue> coreFuture)
        {
            super(new ChunkedResponseIterator<>(coreFuture,
                    pollTimeout,
                    (bucketName) -> new Namespace(type, bucketName),
                    (response) -> response.getBuckets().iterator()));

            this.type = type;
            this.buckets = null;
        }

        public Response(BinaryValue type, List<BinaryValue> buckets)
        {
            this.type = type;
            this.buckets = buckets;
        }

        @Override
        public Iterator<Namespace> iterator()
        {
            if (isStreaming()) {
                return super.iterator();
            }

            assert  buckets != null;
            return new ConvertibleIterator<BinaryValue, Namespace>(buckets.iterator())
            {
                @Override
                protected Namespace convert(BinaryValue bucket)
                {
                    return new Namespace(type, bucket);
                }
            };
        }
    }

    /**
     * Builder for a ListBuckets command.
     */
    public static class Builder
    {
        private int timeout;
        private final BinaryValue type;
        private boolean allowListing;

        /**
         * Construct a Builder for a ListBuckets command.
         * @param type the bucket type.
         */
        public Builder(String type)
        {
            this.type = BinaryValue.create(type);
        }

        /**
         * Allow this listing command
         * <p>
         * Bucket and key list operations are expensive and should not
         * be used in production, however using this method will allow
         * the command to be built.
         * </p>
         * @return a reference to this object.
         */
        public Builder withAllowListing()
        {
            this.allowListing = true;
            return this;
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
        public ListBuckets build() throws ListException
        {
            return new ListBuckets(this);
        }
    }
}
