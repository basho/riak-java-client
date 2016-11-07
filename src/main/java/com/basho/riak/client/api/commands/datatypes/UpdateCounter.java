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

package com.basho.riak.client.api.commands.datatypes;

import com.basho.riak.client.core.operations.DtUpdateOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.crdt.types.RiakCounter;
import com.basho.riak.client.core.query.crdt.types.RiakDatatype;
import com.basho.riak.client.core.util.BinaryValue;

/**
 * Command used to update or create a counter datatype in Riak.
 * <script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>
 * <p>
 * To update or create a counter in Riak you construct a {@link CounterUpdate} and use
 * this command to send it to Riak.
 * <pre class="prettyprint">
 * {@code
 * Namespace ns = new Namespace("my_type", "my_bucket");
 * Location loc = new Location(ns, "my_key");
 * CounterUpdate update = new CounterUpdate(10L);
 *
 * UpdateCounter uc = new UpdateCounter.Builder(loc, update).withReturnDatatype(true).build();
 * UpdateCounter.Response resp = client.execute(uc);
 * RiakCounter counter = resp.getDatatype();
 *
 * }
 * </pre>
 * </p>
 * @author Dave Rusek <drusek at basho dot com>
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class UpdateCounter extends UpdateDatatype<RiakCounter, UpdateCounter.Response, Location>
{
    private UpdateCounter(Builder builder)
    {
        super(builder);
    }

    @Override
    protected Response convertResponse(DtUpdateOperation.Response coreResponse) {
        RiakCounter counter = null;
        if (coreResponse.hasCrdtElement())
        {
            RiakDatatype element = coreResponse.getCrdtElement();
            counter = element.getAsCounter();
        }
        BinaryValue returnedKey = coreResponse.hasGeneratedKey()
            ? coreResponse.getGeneratedKey()
            : null;

        Context returnedCtx = null;
        if (coreResponse.hasContext())
        {
            returnedCtx = new Context(coreResponse.getContext());
        }

        return new Response(returnedCtx, counter, returnedKey);
    }

    /**
     * A response from an UpdateCounter command.
     */
    public static final class Response extends UpdateDatatype.Response<RiakCounter>
    {
        private Response(Context context, RiakCounter datatype, BinaryValue generatedKey)
        {
            super(context, datatype, generatedKey);
        }
    }

    /**
     * Builder used to construct an UpdateCounter command.
     */
    public static final class Builder extends UpdateDatatype.Builder<Builder>
    {
        /**
         * Construct a Builder for an UpdateCounter command.
         * @param location the location of the counter in Riak.
         * @param update the update to apply to the counter.
         */
        public Builder(Location location, CounterUpdate update)
        {
            super(location, update);
            if (update == null)
            {
                throw new IllegalArgumentException("Update cannot be null");
            }
        }

        /**
         * Constructs a builder for an UpdateCounter command with only a Namespace.
         * <p>
         * By providing only a Namespace with the update, Riak will create the
         * counter, generate the key,
         * and return it in the response.
         * </p>
         * @param namespace the namespace to create the datatype.
         * @param update the CounterUpdate to apply
         * @see Response#getGeneratedKey()
         */
        public Builder(Namespace namespace, CounterUpdate update)
        {
            super(namespace, update);
            if (update == null)
            {
                throw new IllegalArgumentException("Update cannot be null");
            }
        }

        /**
         * Construct the UpdateCounter command.
         * @return a new UpdateCounter Command.
         */
        @Override
        public UpdateCounter build()
        {
            return new UpdateCounter(this);
        }

        @Override
        protected Builder self()
        {
            return this;
        }
    }
}
