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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import com.basho.riak.client.raw.ClusterClient;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.config.ClusterConfig;
import com.basho.riak.pbc.RiakClient;
import com.basho.riak.pbc.RiakConnectionPool;

/**
 * @author russell
 * 
 */
public class PBClusterClient extends ClusterClient<PBClientConfig> {

    /**
     * @param clusterConfig
     * @throws IOException
     */
    public PBClusterClient(ClusterConfig<PBClientConfig> clusterConfig) throws IOException {
        super(clusterConfig);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.ClusterClient#fromConfig(com.basho.riak.client
     * .raw.config.ClusterConfig)
     */
    @Override protected RawClient[] fromConfig(ClusterConfig<PBClientConfig> clusterConfig) throws IOException {
        final List<PBClientAdapter> clients = new ArrayList<PBClientAdapter>();

        int totalMaxConnections = clusterConfig.getTotalMaximumConnections();
        Semaphore clusterSemaphore = null;

        if (!(totalMaxConnections <= RiakConnectionPool.LIMITLESS)) {
            // due to the overhead in operating a PoolSemaphore there is no
            // point in
            // creating a LimitlessSemaphore for a cluster, just create
            // Independent pools
            clusterSemaphore = RiakConnectionPool.getSemaphore(totalMaxConnections);
        }

        for (PBClientConfig node : clusterConfig.getNodes()) {
            final RiakConnectionPool hostPool = makePool(clusterSemaphore, node);
            clients.add(new PBClientAdapter(new RiakClient(hostPool)));
        }
        return clients.toArray(new RawClient[clients.size()]);
    }

    /**
     * @param clusterSemaphore
     * @return
     * @throws IOException
     * @throws UnknownHostException
     */
    private RiakConnectionPool makePool(Semaphore clusterSemaphore, PBClientConfig node) throws IOException {
        RiakConnectionPool pool = null;
        if (clusterSemaphore == null) {
            pool = makePool(node);
        } else {
            pool = new RiakConnectionPool(node.getInitialPoolSize(), 
                                          new PoolSemaphore(clusterSemaphore, node.getPoolSize(), true),
                                          InetAddress.getByName(node.getHost()), node.getPort(),
                                          node.getConnectionWaitTimeoutMillis(), node.getSocketBufferSizeKb(),
                                          node.getIdleConnectionTTLMillis());
        }
        return pool;
    }

    /**
     * @param node
     * @return
     */
    private RiakConnectionPool makePool(PBClientConfig node) throws IOException {
        return new RiakConnectionPool(node.getInitialPoolSize(), node.getPoolSize(),
                                      InetAddress.getByName(node.getHost()), node.getPort(),
                                      node.getConnectionWaitTimeoutMillis(), node.getSocketBufferSizeKb(),
                                      node.getIdleConnectionTTLMillis());
    }
}
