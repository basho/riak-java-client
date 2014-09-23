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
import com.basho.riak.client.core.query.crdt.types.RiakMap;
import com.basho.riak.client.core.util.BinaryValue;

/**
 * Command used to update or create a map datatype in Riak.
 * <p>
 * To update or create a map in Riak you construct a {@link MapUpdate} and use
 * this command to send it to Riak.
 * <pre>
 * {@code
 * Namespace ns = new Namespace("my_type", "my_bucket");
 * Location loc = new Location(ns, "my_key");
 * 
 * CounterUpdate cUpdate = new CounterUpdate(10L);
 * MapUpdate update = new MapUpdate().update("my_key", cUpdate);
 * 
 * UpdateMap um = new UpdateMap.Builder(loc, update).withReturnDatatype(true).build();
 * UpdateMap.Response resp = client.execute(um);
 * RiakMap map = resp.getDatatype();
 * 
 * }
 * </pre>
 * </p>
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class UpdateMap extends UpdateDatatype<RiakMap, UpdateMap.Response, Location>
{
    private final MapUpdate update;
    
    private UpdateMap(Builder builder)
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
                    RiakMap map = null;
                    if (coreResponse.hasCrdtElement())
                    {
                        RiakDatatype element = coreResponse.getCrdtElement();
                        map = element.getAsMap();
                    }
                    BinaryValue returnedKey = coreResponse.hasGeneratedKey()
                        ? coreResponse.getGeneratedKey()
                        : null;
                    
                    Context returnedCtx = null;
                    if (coreResponse.hasContext())
                    {
                        returnedCtx = new Context(coreResponse.getContext());
                    }
                    
                    return new Response(returnedCtx, map, returnedKey);
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
    
    public static final class Response extends UpdateDatatype.Response<RiakMap>
    {
        private Response(Context context, RiakMap datatype, BinaryValue generatedKey)
        {
            super(context, datatype, generatedKey);
        }
        
    }
    
    /**
     * Builder used to construct an UpdateMap command.
     */
    public static final class Builder extends UpdateDatatype.Builder<Builder>
    {
        private final MapUpdate update;
        
        /**
         * Construct a Builder for an UpdateMap command.
         * @param location the location of the map in Riak.
         * @param update the update to apply to the map.
         */
        public Builder(Location location, MapUpdate update)
        {
            super(location);
            if (update == null)
            {
                throw new IllegalArgumentException("Update cannot be null");
            }
            this.update = update;
        }

        /**
         * Constructs a builder for an UpdateMap command with only a Namespace.
         * <p>
         * By providing only a Namespace with the update, Riak will create the 
         * map, generate the key, 
         * and return it in the response. 
         * </p>
         * @param namespace the namespace to create the datatype.
         * @param update the update to apply
         * @see Response#getGeneratedKey() 
         */
        public Builder(Namespace namespace, MapUpdate update)
        {
            super(namespace);
            if (update == null)
            {
                throw new IllegalArgumentException("Update cannot be null");
            }
            this.update = update;
        }
        
        @Override
        protected Builder self()
        {
            return this;
        }

        /**
         * Construct a new UpdateMap command.
         * @return a new UpdateMap command.
         */
        @Override
        public UpdateMap build()
        {
            return new UpdateMap(this);
        }

    }
    
}
