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
import com.basho.riak.client.core.operations.DtFetchOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.crdt.types.RiakDatatype;
import com.basho.riak.client.core.query.crdt.types.RiakMap;

/**
 * Command used to fetch a counter datatype from Riak.
 * <script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>
 * <p>
 * <pre class="prettyprint">
 * {@code
 * Namespace ns = new Namespace("my_type", "my_bucket");
 * Location loc = new Location(ns, "my_key");
 * FetchMap fm = new FetchMap.Builder(loc).build();
 * FetchMap.Response resp = client.execute(fm);
 * RiakMap rMap = resp.getDatatype();
 * Map<BinaryValue, List<RiakDatatype>> map = rMap.view();
 * }
 * </pre>
 * </p>
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class FetchMap extends FetchDatatype<RiakMap, FetchMap.Response, Location>
{
	private FetchMap(Builder builder)
	{
		super(builder);
	}

	@Override
	public RiakMap extractDatatype(RiakDatatype element)
	{
		return element.getAsMap();
	}

    @Override
    protected final RiakFuture<FetchMap.Response, Location> executeAsync(RiakCluster cluster)
    {
        RiakFuture<DtFetchOperation.Response, Location> coreFuture =
            cluster.execute(buildCoreOperation());
        
        CoreFutureAdapter<FetchMap.Response, Location, DtFetchOperation.Response, Location> future =
            new CoreFutureAdapter<FetchMap.Response, Location, DtFetchOperation.Response, Location>(coreFuture)
            {
                @Override
                protected FetchMap.Response convertResponse(DtFetchOperation.Response coreResponse)
                {
                    RiakDatatype element = coreResponse.getCrdtElement();

                    Context context = null;
                    if (coreResponse.hasContext())
                    {
                        context = new Context(coreResponse.getContext());
                    }

                    RiakMap datatype = extractDatatype(element);

                    return new Response(datatype, context);
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
     * Builder used to construct a FetchMap command.
     */
	public static class Builder extends FetchDatatype.Builder<Builder>
	{

        /**
         * Construct a Builder for a FetchMap command.
         * @param location the location of the map in Riak.
         */
		public Builder(Location location)
		{
			super(location);
		}

		@Override
		protected Builder self()
		{
			return this;
		}

        /**
         * Construct a new FetchMap command.
         * @return a new FetchMap command.
         */
		public FetchMap build()
		{
			return new FetchMap(this);
		}

	}

    /**
     * Response from a FetchMap command.
     * <p>
     * Encapsulates a RiakMap returned from the command.
     * <pre>
     * {@code
     * ...
     * RiakMap rMap = response.getDatatype();
     * Map<BinaryValue, List<RiakDatatype>> map = rMap.view();
     * 
     * }
     * </pre>
     * </p>
     */
    public static class Response extends FetchDatatype.Response<RiakMap>
    {
        protected Response(RiakMap datatype, Context context)
        {
            super(datatype, context);
        }
    }
    
}
