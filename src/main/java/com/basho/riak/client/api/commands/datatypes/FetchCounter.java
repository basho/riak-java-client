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
import com.basho.riak.client.core.query.crdt.types.RiakCounter;
import com.basho.riak.client.core.query.crdt.types.RiakDatatype;

 /**
 * Command used to fetch a counter datatype from Riak.
 * <script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>
 * <p>
 * <pre class="prettyprint">
 * {@code
 * Namespace ns = new Namespace("my_type", "my_bucket");
 * Location loc = new Location(ns, "my_key");
 * FetchCounter fc = new FetchCounter.Builder(loc).build();
 * FetchCounter.Response resp = client.execute(fc);
 * Long counter = resp.getDatatype().view();}</pre>
 * </p>
 * @author Dave Rusek <drusek at basho dot com>
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public final class FetchCounter extends FetchDatatype<RiakCounter, FetchCounter.Response, Location>
{
	private FetchCounter(Builder builder)
	{
		super(builder);
	}

    @Override
    protected final RiakFuture<FetchCounter.Response, Location> executeAsync(RiakCluster cluster)
    {
        RiakFuture<DtFetchOperation.Response, Location> coreFuture =
            cluster.execute(buildCoreOperation());
        
        CoreFutureAdapter<FetchCounter.Response, Location, DtFetchOperation.Response, Location> future =
            new CoreFutureAdapter<FetchCounter.Response, Location, DtFetchOperation.Response, Location>(coreFuture)
            {
                @Override
                protected FetchCounter.Response convertResponse(DtFetchOperation.Response coreResponse)
                {
                    RiakDatatype element = coreResponse.getCrdtElement();

                    Context context = null;
                    if (coreResponse.hasContext())
                    {
                        context = new Context(coreResponse.getContext());
                    }

                    RiakCounter datatype = extractDatatype(element);

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
	public RiakCounter extractDatatype(RiakDatatype element)
	{
		return element.getAsCounter();
	}

    /**
     * Builder used to construct a FetchCounter command.
     */
	public static class Builder extends FetchDatatype.Builder<Builder>
	{

        /**
         * Construct a builder for a FetchCounter command.
         * @param location the location of the counter in Riak.
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
         * Build a FetchCounter command.
         * @return a new FetchCounter command.
         */
		public FetchCounter build()
		{
			return new FetchCounter(this);
		}
	}
    
    /**
     * Response from a FetchCounter command.
     * <p>
     * Encapsulates a RiakCounter returned from the command. 
     * <pre>
     * {@code 
     * ...
     * RiakCounter counter = response.getDatatype();
     * Long value = counter.view();
     * }
     * </pre>
     * </p>
     */
    public static class Response extends FetchDatatype.Response<RiakCounter>
    {
        Response(RiakCounter counter, Context context)
        {
            super(counter, context);
        }
    }
}
