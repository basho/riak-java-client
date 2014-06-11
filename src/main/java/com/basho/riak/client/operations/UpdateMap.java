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
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.DtUpdateOperation;
import com.basho.riak.client.operations.datatypes.Context;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.operations.datatypes.MapUpdate;
import com.basho.riak.client.query.Namespace;
import com.basho.riak.client.query.crdt.types.RiakDatatype;
import com.basho.riak.client.query.crdt.types.RiakMap;
import com.basho.riak.client.util.BinaryValue;

/**
 *
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
                    RiakDatatype element = coreResponse.getCrdtElement();
                    RiakMap map = element.getAsMap();
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
    
    public static final class Builder extends UpdateDatatype.Builder<Builder>
    {
        private final MapUpdate update;
        
        public Builder(Location location, MapUpdate update)
        {
            super(location);
            if (update == null)
            {
                throw new IllegalArgumentException("Update cannot be null");
            }
            this.update = update;
        }

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

        @Override
        public UpdateMap build()
        {
            return new UpdateMap(this);
        }

    }
    
}
