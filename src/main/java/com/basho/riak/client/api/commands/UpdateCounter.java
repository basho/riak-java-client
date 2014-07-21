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

package com.basho.riak.client.api.commands;

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.DtUpdateOperation;
import com.basho.riak.client.api.commands.datatypes.Context;
import com.basho.riak.client.api.commands.datatypes.CounterUpdate;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.crdt.types.RiakCounter;
import com.basho.riak.client.core.query.crdt.types.RiakDatatype;
import com.basho.riak.client.core.util.BinaryValue;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class UpdateCounter extends UpdateDatatype<RiakCounter, UpdateCounter.Response, Location>
{
    private final CounterUpdate update;
    
    private UpdateCounter(Builder builder)
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

                @Override
                protected Location convertQueryInfo(Location coreQueryInfo)
                {
                    return coreQueryInfo;
                }
                
            };
        coreFuture.addListener(future);
        return future;
    }
    
    public static final class Response extends UpdateDatatype.Response<RiakCounter>
    {
        private Response(Context context, RiakCounter datatype, BinaryValue generatedKey)
        {
            super(context, datatype, generatedKey);
        }
        
    }
    
    public static final class Builder extends UpdateDatatype.Builder<Builder>
    {
        private final CounterUpdate update;
        
        public Builder(Location location, CounterUpdate update)
        {
            super(location);
            if (update == null)
            {
                throw new IllegalArgumentException("Update cannot be null");
            }
            this.update = update;
        }

        public Builder(Namespace namespace, CounterUpdate update)
        {
            super(namespace);
            if (update == null)
            {
                throw new IllegalArgumentException("Update cannot be null");
            }
            this.update = update;
        }
        
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
