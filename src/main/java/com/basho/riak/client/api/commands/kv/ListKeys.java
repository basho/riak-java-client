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
package com.basho.riak.client.api.commands.kv;

import com.basho.riak.client.api.ListException;
import com.basho.riak.client.api.StreamableRiakCommand;
import com.basho.riak.client.api.commands.ChunkedResponseIterator;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.StreamingRiakFuture;
import com.basho.riak.client.core.operations.ListKeysOperation;
import com.basho.riak.client.core.query.ConvertibleIterator;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;

import java.util.Iterator;
import java.util.List;

/**
 * Command used to list the keys in a bucket.
 * <script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>
 * <p>
 * This command is used to retrieve a list of all keys in a bucket. The response
 * is iterable and contains a list of Locations.
 * <pre class="prettyprint">
 * {@code
 * Namespace ns = new Namespace("my_type", "my_bucket");
 * ListKeys lk = new ListKeys.Builder(ns).build();
 * ListKeys.Response response = client.execute(lk);
 * for (Location l : response)
 * {
 *     System.out.println(l.getKeyAsString());
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
 * Namespace ns = new Namespace("my_type", "my_bucket");
 * ListKeys lk = new ListKeys.Builder(ns).build();
 * RiakFuture<ListKeys.StreamingResponse, Namespace> streamFuture =
 *     client.executeAsyncStreaming(lk, 200);
 * final ListKeys.StreamingResponse streamingResponse = streamFuture.get();
 * ListKeys.Response response = client.execute(lk);
 * for (Location l : streamingResponse)
 * {
 *     System.out.println(l.getKeyAsString());
 * }}</pre>
 * </p>
 * <p>
 * <b>This is a very expensive operation and is not recommended for use on a production system</b>
 * </p>
 *
 * @author Dave Rusek <drusek at basho dot com>
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0
 */
public final class ListKeys extends StreamableRiakCommand.StreamableRiakCommandWithSameInfo<ListKeys.Response,
        Namespace, ListKeysOperation.Response>
{
    private final Namespace namespace;
    private final int timeout;

    ListKeys(Builder builder) throws ListException
    {
        this.namespace = builder.namespace;
        this.timeout = builder.timeout;

        if (builder.allowListing == false)
        {
            throw new ListException();
        }
    }

    @Override
    protected Response convertResponse(FutureOperation<ListKeysOperation.Response, ?, Namespace> request,
                                       ListKeysOperation.Response coreResponse)
    {
        return new Response(namespace, coreResponse.getKeys());
    }

    @Override
    protected Response createResponse(int timeout, StreamingRiakFuture<ListKeysOperation.Response, Namespace> coreFuture)
    {
        return new Response(namespace, timeout, coreFuture);
    }

    @Override
    protected ListKeysOperation buildCoreOperation(boolean streamResults)
    {
        ListKeysOperation.Builder builder = new ListKeysOperation.Builder(namespace);

        if (timeout > 0)
        {
            builder.withTimeout(timeout);
        }

        builder.streamResults(streamResults);

        return builder.build();
    }

    public static class Response extends StreamableRiakCommand.StreamableResponse<Location, BinaryValue>
    {
        private final Namespace namespace;
        private final List<BinaryValue> keys;

        public Response(Namespace namespace, List<BinaryValue> keys)
        {
            this.namespace = namespace;
            this.keys = keys;
        }

        Response(Namespace namespace,
                          int pollTimeout,
                          StreamingRiakFuture<ListKeysOperation.Response, Namespace> coreFuture)
        {
            super(new ChunkedResponseIterator<>(coreFuture,
                    pollTimeout,
                    (key) -> new Location(namespace, key),
                    (nextChunk) -> nextChunk.getKeys().iterator()));

            this.namespace = namespace;
            this.keys = null;
        }

        @Override
        public Iterator<Location> iterator()
        {
            if (isStreaming())
            {
                return super.iterator();
            }

            assert keys != null;
            return new ConvertibleIterator<BinaryValue, Location>(keys.iterator())
            {
                @Override
                protected Location convert(BinaryValue key)
                {
                    return new Location(namespace, key);
                }
            };
        }
    }

    /**
     * Used to construct a ListKeys command.
     */
    public static class Builder
    {
        private final Namespace namespace;
        private int timeout;
        private boolean allowListing;

        /**
         * Constructs a Builder for a ListKeys command.
         * @param namespace the namespace from which to list keys.
         */
        public Builder(Namespace namespace)
        {
            if (namespace == null)
            {
                throw new IllegalArgumentException("Namespace cannot be null");
            }
            this.namespace = namespace;
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
         * Construct the ListKeys command.
         * @return A ListKeys command.
         */
        public ListKeys build() throws ListException
        {
            return new ListKeys(this);
        }
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (namespace != null ? namespace.hashCode() : 0);
        result = prime * result + timeout;
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null)
        {
            return false;
        }
        if (!(obj instanceof ListKeys))
        {
            return false;
        }

        final ListKeys other = (ListKeys) obj;
        if (this.namespace != other.namespace && (this.namespace == null || !this.namespace.equals(other.namespace)))
        {
            return false;
        }
        if (this.timeout != other.timeout)
        {
            return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        return String.format("{namespace: %s, timeout: %s}", namespace, timeout);
    }
}
