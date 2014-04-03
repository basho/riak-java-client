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
import com.basho.riak.client.operations.datatypes.SetUpdate;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.crdt.types.RiakDatatype;
import com.basho.riak.client.query.crdt.types.RiakSet;
import com.basho.riak.client.util.BinaryValue;

/**
 *
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
                    RiakDatatype element = coreResponse.getCrdtElement();
                    RiakSet set = element.getAsSet();
                    BinaryValue returnedKey = coreResponse.hasGeneratedKey()
                        ? coreResponse.getGeneratedKey()
                        : null;
                    Context returnedCtx = new Context(coreResponse.getContext().getValue());
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
    
    
    public class Builder extends UpdateDatatype.Builder<Builder>
    {
        private final SetUpdate update;
        
        public Builder(Location loc, SetUpdate update)
        {
            super(loc);
            if (update == null)
            {
                throw new IllegalArgumentException("Update cannot be null");
            }
            this.update = update;
        }
        
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
    
    public static class Response extends UpdateDatatype.Response<RiakSet>
    {
        private Response(Context context, RiakSet datatype, BinaryValue generatedKey)
        {
            super(context, datatype, generatedKey);
        }
        
    }
}
