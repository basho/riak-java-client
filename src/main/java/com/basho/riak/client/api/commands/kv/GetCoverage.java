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

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.api.commands.CoreFutureAdapter;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.GetCoverageOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Command used to get a coverage information from Riak.
 * <p>
 * It returns coverage for the specified {@link com.basho.riak.client.core.query.Location} or
 * {@link com.basho.riak.client.core.query.Namespace}.
 * if nothing was specified then the full coverage will be returned.
 * <p>
 * Coverage is returned as a {@link List} of Riak entry points (Riak nodes), which are defined as a {@link java.util.Map.Entry}:
 * <ul>
 *  <li>{@link Map.Entry#getKey()} contains address of the corresponding endpoint</li>
 *  <li> {@link Map.Entry#getValue()} contains port</li>
 * </ul>
 *
 * <b>Note:</b> The endpoint's address is always returned as IPv4 in dot-decimal notation.
 *
 * @author Sergey Galkin <sgalkin at basho dot com>
 */
public class GetCoverage extends RiakCommand<GetCoverage.Response, String>{
    private final Namespace namespace;
    private final BinaryValue key;
    private final boolean forceUpdte;

    private GetCoverage(Builder builder){
        this.namespace = builder.namespace;
        this.key = builder.key;
        this.forceUpdte = builder.forceUpdate;
    }

    @Override
    protected RiakFuture<GetCoverage.Response, String> executeAsync(RiakCluster cluster) {
        RiakFuture<GetCoverageOperation.Response , String> coreFuture =
                cluster.execute(buildCoreOperation());

        CoreFutureAdapter<GetCoverage.Response, String, GetCoverageOperation.Response, String> future =
                new CoreFutureAdapter<GetCoverage.Response, String, GetCoverageOperation.Response, String>(coreFuture)
                {
                    @Override
                    protected Response convertResponse(GetCoverageOperation.Response coreResponse)
                    {
                        return new Response(coreResponse.geEntryPoints());
                    }

                    @Override
                    protected String convertQueryInfo(String coreQueryInfo) {
                        return coreQueryInfo;
                    }
                };
        coreFuture.addListener(future);
        return future;
    }

    private GetCoverageOperation buildCoreOperation()
    {
        final GetCoverageOperation.Builder coreBuilder;
        if(key != null)
        {
            // Location coverage
            coreBuilder = new GetCoverageOperation.Builder(new Location(namespace, key));
        }
        else if(namespace != null)
        {
            // Namespace coverage
            coreBuilder = new GetCoverageOperation.Builder(namespace);
        }
        else
        {
            // Full coverage
            coreBuilder = new GetCoverageOperation.Builder();
        }

        if(forceUpdte)
        {
            coreBuilder.withForcedUpdate();
        }

        return coreBuilder.build();
    }

    /**
     * A response from Riak containing results from a GetCoverage command.
     */
    public static class Response implements Iterable<Map.Entry<String,Integer>>{
        final List<Map.Entry<String,Integer>> entryPoints;
        public Response(List<Map.Entry<String,Integer>> pairs) {
            this.entryPoints = pairs;
        }

        @Override
        public Iterator<Map.Entry<String,Integer>> iterator() {
            return entryPoints.iterator();
        }

        public List<Map.Entry<String,Integer>> entryPoints(){
            return entryPoints;
        }
    }

    /**
     * Used to construct a GetCoverage command.
     */
    public static class Builder{
        private Namespace namespace;
        private BinaryValue key;
        private boolean forceUpdate;

        /**
         * Set the namespace.
         * @param namespace The {@link com.basho.riak.client.core.query.Namespace}.
         * @return a reference to this object.
         */
        public Builder withNamespace(Namespace namespace)
        {
            this.namespace = namespace;
            return this;
        }

        /**
         * Set the location.
         * @param location the {@link com.basho.riak.client.core.query.Location}.
         * @return a reference to this object.
         */
        public Builder withLocation(Location location)
        {
            this.namespace = location.getNamespace();
            this.key = location.getKey();
            return this;
        }

        /**
         * Force entry points discovering on the actual nodes (that is costly), even when Riak already has
         * corresponding result cached from the previous call.
         * <p>
         * <b>Note:</b> If you are not sure what it is, use it. Most likely it will prevent you from spending hours
         * trying to realize what the strange behavior you have got.
         *
         * @return a reference to this object.
         */
        public Builder withForcedUpdate(){
            this.forceUpdate = true;
            return this;
        }

        /**
         * Construct the  GetCoverage command.
         * @return the new GetCoverage command.
         */
        public GetCoverage build()
        {
            return new  GetCoverage(this);
        }
    }
}
