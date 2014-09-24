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

import com.basho.riak.client.api.commands.CoreFutureAdapter;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.DtUpdateOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.crdt.types.RiakDatatype;
import com.basho.riak.client.core.query.crdt.types.RiakSet;
import com.basho.riak.client.core.util.BinaryValue;

/**
 * Command used to update or create a set datatype in Riak.
 * <script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>
 * <p>
 * To update or create a set in Riak you construct a {@link SetUpdate} and use
 * this command to send it to Riak.
 * <pre class="prettyprint">
 * {@code 
 * Namespace ns = new Namespace("my_type", "my_bucket");
 * Location loc = new Location(ns, "my_key");
 * SetUpdate update = new SetUpdate().add("some_new_value");
 * 
 * UpdateSet us = new UpdateSet.Builder(loc, update).withReturnDatatype(true).build();
 * UpdateSet.Response resp = client.execute(us);
 * RiakSet = resp.getDatatype();
 * }
 * </pre>
 * </p>
 * 
 * @author Dave Rusek <drusek at basho dot com>
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class UpdateSet extends UpdateDatatype<RiakSet, UpdateSet.Response, Location>
{
    private final SetUpdate update;
    
    private UpdateSet(Builder builder)
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
                    RiakSet set = null;
                    if (coreResponse.hasCrdtElement())
                    {
                        RiakDatatype element = coreResponse.getCrdtElement();
                        set = element.getAsSet();
                    }
                    BinaryValue returnedKey = coreResponse.hasGeneratedKey()
                        ? coreResponse.getGeneratedKey()
                        : null;
                    Context returnedCtx = null;
                    if (coreResponse.hasContext())
                    {
                        returnedCtx = new Context(coreResponse.getContext());
                    }
                    return new Response(returnedCtx, set, returnedKey);
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
     * Builder used to construct an UpdateSet command.
     */
    public static class Builder extends UpdateDatatype.Builder<Builder>
    {
        private final SetUpdate update;
        
        /**
         * Construct a Builder for an UpdateSet command.
         * @param location the location of the set in Riak.
         * @param update the update to apply to the set.
         */
        public Builder(Location location, SetUpdate update)
        {
            super(location);
            if (update == null)
            {
                throw new IllegalArgumentException("Update cannot be null");
            }
            this.update = update;
        }
        
        /**
         * Constructs a builder for an UpdateSet command with only a Namespace.
         * <p>
         * By providing only a Namespace with the update, Riak will create the 
         * set, generate the key, 
         * and return it in the response. 
         * </p>
         * @param namespace the namespace to create the datatype.
         * @param update the update to apply
         * @see Response#getGeneratedKey() 
         */
        public Builder(Namespace namespace, SetUpdate update)
        {
            super(namespace);
            if (update == null)
            {
                throw new IllegalArgumentException("Update cannot be null");
            }
            this.update = update;
        }
        
        /**
         * Construct a new UpdateSet command.
         * @return a new UpdateSet command.
         */
        @Override
        public UpdateSet build()
        {
            return new UpdateSet(this);
        }
        
        @Override
        protected Builder self()
        {
            return this;
        }
        
    }
    
    /**
     * A response from an UpdateSet command.
     */
    public static class Response extends UpdateDatatype.Response<RiakSet>
    {
        private Response(Context context, RiakSet datatype, BinaryValue generatedKey)
        {
            super(context, datatype, generatedKey);
        }
        
    }
}
