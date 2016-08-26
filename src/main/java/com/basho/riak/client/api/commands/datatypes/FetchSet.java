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
import com.basho.riak.client.core.query.crdt.types.RiakSet;

/**
 * Command used to fetch a set datatype from Riak.
 * <script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>
 * <p>
 * <pre class="prettyprint">
 * {@code
 * Namespace ns = new Namespace("my_type", "my_bucket");
 * Location loc = new Location(ns, "my_key");
 * FetchSet fs = new FetchSet.Builder(loc).build();
 * FetchSet.Response resp = client.execute(fs);
 * RiakSet rSet = resp.getDatatype();
 * Set<BinaryValue> set = rSet.view();
 * }
 * </pre>
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
            new CoreFutureAdapter<FetchSet.Response, Location, DtFetchOperation.Response, Location>(coreFuture)
            {
                @Override
                protected FetchSet.Response convertResponse(DtFetchOperation.Response coreResponse)
                {
                    RiakDatatype element = coreResponse.getCrdtElement();

                    Context context = null;
                    if (coreResponse.hasContext())
                    {
                        context = new Context(coreResponse.getContext());
                    }

                    RiakSet datatype = extractDatatype(element);

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
    
	@Override
	public RiakSet extractDatatype(RiakDatatype element)
	{
		return element.getAsSet();
	}

    /**
     * Builder used to construct a FetchSet command.
     */
	public static class Builder extends FetchDatatype.Builder<Builder>
	{

		/**
         * Construct a builder for a FetchSet command.
         * @param location the location of the set in Riak.
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
         * Construct a FetchSet command.
         * @return a new FetchSet Command.
         */
		public FetchSet build()
		{
			return new FetchSet(this);
		}
	}
    
    /**
     * Response from a FetchSet command.
     * <p>
     * Encapsulates a RiakSet returned from the FetchSet command.
     * <pre>
     * {@code
     * ...
     * RiakSet rSet = response.getDatatype();
     * Set<BinaryValue> set = rSet.view();
     * }
     * </pre>
     * </p>
     */
    public static class Response extends FetchDatatype.Response<RiakSet>
    {
        Response(RiakSet set, Context context)
        {
            super(set,context);
        }
    }
}
