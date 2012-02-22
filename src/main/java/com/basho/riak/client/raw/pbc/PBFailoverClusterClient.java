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

import com.basho.riak.client.raw.FailoverClusterClient;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.Transport;
import com.basho.riak.client.raw.config.ClusterConfig;
import com.basho.riak.pbc.RiakClient;
import com.basho.riak.pbc.RiakConnectionPool;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 * Temetra
 * pbarry 12/02/2012 11:17
 */
public class PBFailoverClusterClient extends FailoverClusterClient<PBClientConfig>
{
  /**
   * @param clusterConfig
   * @throws java.io.IOException
   */
  public PBFailoverClusterClient(ClusterConfig<PBClientConfig> clusterConfig) throws IOException
  {
      super(clusterConfig);
  }

  @Override protected RawClient[] fromConfig(ClusterConfig<PBClientConfig> clusterConfig) throws IOException {
      final List<PBClientAdapter> clients = new ArrayList<PBClientAdapter>();

      int totalMaxConnections = clusterConfig.getTotalMaximumConnections();
      Semaphore clusterSemaphore = null;

      if (totalMaxConnections > RiakConnectionPool.LIMITLESS) {
          // due to the overhead in operating a PoolSemaphore there is no
          // point in
          // creating a LimitlessSemaphore for a cluster, just create
          // Independent pools
          clusterSemaphore = RiakConnectionPool.getSemaphore(totalMaxConnections);
      }

      for (PBClientConfig node : clusterConfig.getClients()) {
          final RiakConnectionPool hostPool = makePool(clusterSemaphore, node);
          hostPool.start();
          clients.add(new PBClientAdapter(new RiakClient(hostPool)));
      }
      return clients.toArray(new RawClient[clients.size()]);
  }

  /**
   * Creates the {@link RiakConnectionPool} for the given client config
   *
   * @param clusterSemaphore
   *            a {@link Semaphore} or null if none required (i.e. unbounded
   *            cluster pool)
   * @return a {@link RiakConnectionPool} configured for the
   *         {@link PBClientConfig}
   * @throws IOException
   */
  private RiakConnectionPool makePool(Semaphore clusterSemaphore, PBClientConfig node) throws IOException {
      RiakConnectionPool pool = null;
      if (clusterSemaphore == null) {
          pool = new RiakConnectionPool(node.getInitialPoolSize(), node.getPoolSize(),
                                        InetAddress.getByName(node.getHost()), node.getPort(),
                                        node.getConnectionWaitTimeoutMillis(), node.getSocketBufferSizeKb(),
                                        node.getIdleConnectionTTLMillis());
      } else {
          pool = new RiakConnectionPool(node.getInitialPoolSize(), new PoolSemaphore(clusterSemaphore,
                                                                                     node.getPoolSize()),
                                        InetAddress.getByName(node.getHost()), node.getPort(),
                                        node.getConnectionWaitTimeoutMillis(), node.getSocketBufferSizeKb(),
                                        node.getIdleConnectionTTLMillis());
      }
      return pool;
  }

  public Transport getTransport() {
      return Transport.PB;
  }
}
