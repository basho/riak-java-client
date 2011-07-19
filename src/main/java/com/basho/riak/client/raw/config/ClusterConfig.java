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

import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.pbc.PBClientConfig;

/**
 * @author russell
 * @param <T>
 * 
 */
public abstract class ClusterConfig<T> implements Configuration {

    private final int totalMaximumConnections;
    private final List<T> nodes = new ArrayList<T>();

    /**
     * @param totalMaximumConnections
     * @param maxConnectionsPerHost
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
     * Add a new node config to the collection of nodes in the cluster config
     * 
     * @param nodeConfig
     *            a node config to add
     * @return this, updated
     * @see HTTPClientConfig
     * @see PBClientConfig
     */
    public synchronized ClusterConfig<T> addNode(T nodeConfig) {
        this.nodes.add(nodeConfig);
        return this;
    }

    /**
     * 
     * @return an *unmodifiable* view of the node configs in the cluster config
     */
    public synchronized List<T> getNodes() {
        return Collections.unmodifiableList(nodes);
    }
}
