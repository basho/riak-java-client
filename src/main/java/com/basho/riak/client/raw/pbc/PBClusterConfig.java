/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.client.raw.pbc;

import com.basho.riak.client.raw.config.ClusterConfig;
import com.basho.riak.client.raw.pbc.PBClientConfig.Builder;

/**
 * {@link ClusterConfig} implementation that provides {@link PBClientConfig} as
 * the generic type.
 * 
 * @author russell
 * 
 */
public class PBClusterConfig extends ClusterConfig<PBClientConfig> {

    /**
     * @param totalMaximumConnections
     */
    public PBClusterConfig(int totalMaximumConnections) {
        super(totalMaximumConnections);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.raw.config.ClusterConfig#forHosts(java.lang.String[])
     */
    @Override public ClusterConfig<PBClientConfig> addHosts(String... hosts) {
        return addHosts(PBClientConfig.defaults(), hosts);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.raw.config.ClusterConfig#forHosts(com.basho.riak.client.raw.config.Configuration, java.lang.String[])
     */
    @Override public ClusterConfig<PBClientConfig> addHosts(PBClientConfig config, String... hosts) {
        Builder b = PBClientConfig.Builder.from(config);

        for(String host : hosts) {
            addClient(b.withHost(host).build());
        }

        return this;
    }

}
