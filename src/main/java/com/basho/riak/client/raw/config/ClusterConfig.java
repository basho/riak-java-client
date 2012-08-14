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
package com.basho.riak.client.raw.config;


import com.basho.riak.client.raw.DefaultDelegateProvider;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.DelegateProvider;

/**
 * Abstract parent {@link Configuration} for a "cluster" of clients.
 * <p>
 * Holds a collection of {@link Configuration}s, one for each node in a cluster.
 * Currently only supports homogeneous clusters.
 * </p>
 * 
 * @author russell
 * @param <T>
 *            concrete {@link Configuration} type
 * 
 */
public abstract class ClusterConfig<T extends Configuration> implements Configuration {

    /**
     * Constant for specifying unlimited maximum connection
     */
    public static final int UNLIMITED_CONNECTIONS = 0;

    private final int totalMaximumConnections;
    private final List<T> nodes = new ArrayList<T>();
    private DelegateProvider delegateProvider = new DefaultDelegateProvider();

    /**
     * @param totalMaximumConnections
     *            the upper limit of connections for all nodes in the config
     *            NOTE: set individual client limits in each client config
     */
    public ClusterConfig(int totalMaximumConnections) {
        this.totalMaximumConnections = totalMaximumConnections;
    }

    /**
     * @return the totalMaximumConnections
     */
    public int getTotalMaximumConnections() {
        return totalMaximumConnections;
    }

    public synchronized ClusterConfig<T> setDelegateProvider(DelegateProvider delegateProvider)
    {
        this.delegateProvider = delegateProvider;
        return this;
    }
    
    public synchronized DelegateProvider getDelegateProvider()
    {
        return this.delegateProvider;
    }
    
    /**
     * Add a new client config to the collection of client in the cluster config
     * 
     * @param nodeConfig
     *            a node config to add
     * @return this, updated
     * @see HTTPClientConfig
     * @see PBClientConfig
     */
    public synchronized ClusterConfig<T> addClient(T clientConfig) {
        this.nodes.add(clientConfig);
        return this;
    }

    /**
     * 
     * @return an *unmodifiable* view of the client {@link Configuration}s in
     *         the cluster configuration
     */
    public synchronized List<T> getClients() {
        return Collections.unmodifiableList(nodes);
    }

    /**
     * Convenience method for creating a cluster of hosts with a common, default
     * config except for host
     * 
     * @param hosts
     *            var arg String array of hosts
     * @return the {@link ClusterConfig} with a node for each host in
     *         <code>hosts</code>
     */
    protected abstract ClusterConfig<T> addHosts(String... hosts);

    /**
     * Convenience method for creating a cluster of hosts with a common
     * config except for host
     * 
     * @param config T the common base config
     * @param hosts
     *            var arg String array of hosts
     * @return the {@link ClusterConfig} with a node for each host in
     *         <code>hosts</code>
     */
    protected abstract ClusterConfig<T> addHosts(T config, String... hosts);
}
