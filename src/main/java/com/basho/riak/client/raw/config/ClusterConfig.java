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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.basho.riak.client.raw.cluster.ClusterObserver;
import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.pbc.PBClientConfig;

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
    private long healthCheckFrequencyMillis = TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS);
    private final List<T> nodes = new ArrayList<T>();
    private final List<ClusterObserver> clusterObservers = new ArrayList<ClusterObserver>();

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

    /**
     * Add a new client config to the collection of client in the cluster config
     * 
     * @param clientConfig
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
     * Add a new observer to receive notifications when the state of the cluster changes
     *
     * @param clusterObserver
     *          a {@link ClusterObserver} to add
     * @return this, updated
     * @see ClusterObserver
     */
    public synchronized ClusterConfig<T> addClusterObserver(ClusterObserver clusterObserver) {
        this.clusterObservers.add(clusterObserver);
        return this;
    }

    /**
     *
     * @return an *unmodifiable* view of the {@link ClusterObserver}s in
     *         the cluster configuration
     */
    public synchronized List<ClusterObserver> getClusterObservers() {
        return Collections.unmodifiableList(clusterObservers);
    }

    /**
     * Set the frequency with which to check to see if unhealthy nodes have recovered
     *
     * @param duration the frequency with which to check
     * @param timeUnit the time unit of the frequency
     * @return this, updated
     */
    public ClusterConfig<T> setHealthCheckFrequency(long duration, TimeUnit timeUnit) {
        this.healthCheckFrequencyMillis = TimeUnit.MILLISECONDS.convert(duration, timeUnit);
        return this;
    }

    /**
     * Get the frequency with which to check to see if unhealthy nodes have recovered (in millis)
     *
     * @return the frequency with which to check (in millis)
     */
    public long getHealthCheckFrequencyMillis() {
        return this.healthCheckFrequencyMillis;
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
