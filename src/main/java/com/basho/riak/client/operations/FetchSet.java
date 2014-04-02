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
import com.basho.riak.client.core.operations.DtFetchOperation;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.crdt.types.RiakDatatype;
import com.basho.riak.client.query.crdt.types.RiakSet;
import com.basho.riak.client.util.BinaryValue;

 /*
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class FetchSet extends FetchDatatype<RiakSet, FetchSet.Response, Location>
{
	private FetchSet(Builder builder)
	{
		super(builder);
	}

    @Override
    protected final RiakFuture<FetchSet.Response, Location> executeAsync(RiakCluster cluster)
    {
        RiakFuture<DtFetchOperation.Response, Location> coreFuture =
            cluster.execute(buildCoreOperation());
        
        CoreFutureAdapter<FetchSet.Response, Location, DtFetchOperation.Response, Location> future =
            new CoreFutureAdapter<FetchSet.Response, Location, DtFetchOperation.Response, Location>(coreFuture) {

            @Override
            protected FetchSet.Response convertResponse(DtFetchOperation.Response coreResponse)
            {
                RiakDatatype element = coreResponse.getCrdtElement();
                BinaryValue context = coreResponse.getContext();

                RiakSet datatype = extractDatatype(element);

                return new Response(datatype, context.getValue());
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
    
	@Override
	public RiakSet extractDatatype(RiakDatatype element)
	{
		return element.getAsSet();
	}

	public static class Builder extends FetchDatatype.Builder<Builder>
	{

		public Builder(Location location)
		{
			super(location);
		}

		@Override
		protected Builder self()
		{
			return this;
		}

		public FetchSet build()
		{
			return new FetchSet(this);
		}
	}
    
    public static class Response extends FetchDatatype.Response<RiakSet>
    {
        Response(RiakSet set, byte[] context)
        {
            super(set,context);
        }
    }
}
