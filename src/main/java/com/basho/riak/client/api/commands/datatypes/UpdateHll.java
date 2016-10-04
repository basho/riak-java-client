/*
 * Copyright 2016 Basho Technologies Inc
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

package com.basho.riak.client.api.commands.datatypes;

import com.basho.riak.client.api.commands.CoreFutureAdapter;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.DtUpdateOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.crdt.types.RiakDatatype;
import com.basho.riak.client.core.query.crdt.types.RiakHll;
import com.basho.riak.client.core.util.BinaryValue;

/**
 * Command used to update or create a HyperLogLog datatype in Riak.
 * <script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>
 * <p>
 * To update or create a HyperLogLog in Riak you construct a {@link HllUpdate} and use
 * this command to send it to Riak.
 * <pre class="prettyprint">
 * {@code
 *     Namespace ns = new Namespace("my_type", "my_bucket");
 *     Location loc = new Location(ns, "my_key");
 *     HllUpdate update = new HllUpdate().add("some_new_value");
 *
 *     UpdateHll us = new UpdateHll.Builder(loc, update).withReturnDatatype(true).build();
 *     UpdateHll.Response resp = client.execute(us);
 *     RiakHll = resp.getDatatype();
 * }
 * </pre>
 * </p>
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.1
 */
public class UpdateHll extends UpdateDatatype<RiakHll, UpdateHll.Response, Location>
{
    private final HllUpdate update;

    private UpdateHll(Builder builder)
    {
        super(builder);
        this.update = builder.update;
    }

    @Override
    protected RiakFuture<Response, Location> executeAsync(RiakCluster cluster)
    {
        RiakFuture<DtUpdateOperation.Response, Location> coreFuture =
            cluster.execute(buildCoreOperation(update));

        CoreFutureAdapter<Response, Location, DtUpdateOperation.Response, Location> future =
            new CoreFutureAdapter<Response, Location, DtUpdateOperation.Response, Location>(coreFuture)
            {
                @Override
                protected Response convertResponse(DtUpdateOperation.Response coreResponse)
                {
                    RiakHll hll = null;
                    if (coreResponse.hasCrdtElement())
                    {
                        RiakDatatype element = coreResponse.getCrdtElement();
                        hll = element.getAsHll();
                    }
                    BinaryValue returnedKey = coreResponse.hasGeneratedKey()
                        ? coreResponse.getGeneratedKey()
                        : null;
                    Context returnedCtx = null;
                    if (coreResponse.hasContext())
                    {
                        returnedCtx = new Context(coreResponse.getContext());
                    }
                    return new Response(returnedCtx, hll, returnedKey);
                }

                @Override
                protected Location convertQueryInfo(Location coreQueryInfo)
                {
                    return coreQueryInfo;
                }
            };
        coreFuture.addListener(future);
        return future;
    }

    /**
     * Builder used to construct an UpdateHll command.
     */
    public static class Builder extends UpdateDatatype.Builder<Builder>
    {
        private final HllUpdate update;

        /**
         * Construct a Builder for an UpdateHll command.
         * @param location the location of the HyperLogLog in Riak.
         * @param update the update to apply to the HyperLogLog.
         */
        public Builder(Location location, HllUpdate update)
        {
            super(location);
            if (update == null)
            {
                throw new IllegalArgumentException("Update cannot be null");
            }
            this.update = update;
        }

        /**
         * Constructs a builder for an UpdateHll command with only a Namespace.
         * <p>
         * By providing only a Namespace with the update, Riak will create the
         * HyperLogLog, generate the key, and return it in the response.
         * </p>
         * @param namespace the namespace to create the datatype.
         * @param update the update to apply
         * @see Response#getGeneratedKey()
         */
        public Builder(Namespace namespace, HllUpdate update)
        {
            super(namespace);
            if (update == null)
            {
                throw new IllegalArgumentException("Update cannot be null");
            }
            this.update = update;
        }

        /**
         * Contexts are not used with the Hyperloglog data type
         * @param unused Unused parameter
         * @return a copy of this object
         */
        @Override
        public Builder withContext(Context unused)
        {
            return this;
        }

        /**
         * Construct a new UpdateHll command.
         * @return a new UpdateHll command.
         */
        @Override
        public UpdateHll build()
        {
            return new UpdateHll(this);
        }

        @Override
        protected Builder self()
        {
            return this;
        }
    }

    /**
     * A response from an UpdateHll command.
     */
    public static class Response extends UpdateDatatype.Response<RiakHll>
    {
        private Response(Context context, RiakHll datatype, BinaryValue generatedKey)
        {
            super(context, datatype, generatedKey);
        }

        /**
         * Contexts are not used with the Hyperloglog data type
         * @return false
         */
        @Override
        public boolean hasContext()
        {
            return false;
        }

        /**
         * Contexts are not used with the Hyperloglog data type
         * @return null
         */
        @Override
        public Context getContext()
        {
            return null;
        }
    }
}
