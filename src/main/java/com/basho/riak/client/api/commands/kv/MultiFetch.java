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
package com.basho.riak.client.api.commands.kv;

import com.basho.riak.client.api.commands.kv.FetchValue.Option;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.query.Location;

import java.util.List;

/**
 * Command used to fetch multiple values from Riak.
 * <script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>
 * <p>
 * Riak itself does not support pipelining of requests. MutliFetch addresses this issue by using a thread to
 * parallelize and manage a set of async fetch operations for a given set of keys.
 * </p>
 * <p>
 * The result of executing this command is a {@code List} of {@link RiakFuture} objects, each one representing a single
 * fetch operation. The returned {@code RiakFuture} that contains that list completes
 * when all the FetchValue operations contained have finished.
 * <p/>
 * <pre class="prettyprint">
 * {@code
 * MultiFetch multifetch = ...;
 * MultiFetch.Response response = client.execute(multifetch);
 * List<MyPojo> myResults = new ArrayList<MyPojo>();
 * for (RiakFuture<FetchValue.Response, Location> f : response)
 * {
*     try
*     {
*          FetchValue.Response response = f.get();
*          myResults.add(response.getValue(MyPojo.class));
*     }
*     catch (ExecutionException e)
*     {
*         // log error, etc.
*     }
 * }}</pre>
 * </p>
 * <p>
 * The maximum number of concurrent requests defaults to 10. This can be changed
 * when constructing the operation.
 * </p>
 * <p>
 * Be aware that because requests are being parallelized performance is also
 * dependent on the client's underlying connection pool. If there are no connections
 * available performance will suffer initially as connections will need to be established
 * or worse they could time out.
 * </p>
 *
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class MultiFetch extends MultiCommand<FetchValue, FetchValue.Builder, MultiFetch.Response, FetchValue.Response>
{
    private MultiFetch(Builder builder)
    {
        super(builder);
    }

    @Override
    protected Response createResponseType(List<RiakFuture<FetchValue.Response, Location>> riakFutures)
    {
        return new Response(riakFutures);
    }

    @Override
    protected FetchValue.Builder createBaseBuilderType(Location location)
    {
        return new FetchValue.Builder(location);
    }

    @Override
    protected RiakFuture<FetchValue.Response, Location> executeBaseCommandAsync(FetchValue command, RiakCluster cluster)
    {
        return command.executeAsync(cluster);
    }

    /**
     * Used to construct a MutiFetch command.
     */
    public static class Builder extends MultiCommand.Builder<MultiFetch, Builder>
    {
        /**
         * Set the Riak-side timeout value.
         * <p>
         * By default, riak has a 60s timeout for operations. Setting
         * this value will override that default for each fetch.
         * </p>
         *
         * @param timeout the timeout in milliseconds to be sent to riak.
         * @return a reference to this object.
         */
        public Builder withTimeout(int timeout)
        {
            withOption(Option.TIMEOUT, timeout);
            return this;
        }

        /**
         * Build a {@link MultiFetch} operation from this builder
         *
         * @return an initialized {@link MultiFetch} operation
         */
        public MultiFetch build()
        {
            return new MultiFetch(this);
        }

        @Override
        protected Builder self()
        {
            return this;
        }
    }

    /**
     * The response from Riak for a MultiFetch command.
     */
    public static class Response extends MultiCommand.Response<FetchValue.Response>
    {
        Response(List<RiakFuture<FetchValue.Response, Location>> responses)
        {
            super(responses);
        }
    }
}
