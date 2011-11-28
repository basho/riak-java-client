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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterConfig;

/**
 * @author russell
 * 
 */
public class ClusterConfigTest {

    private static final String[] HOSTS = new String[] { "localhost", "remotehost1", "remotehost2" };

    private PBClusterConfig clusterConfig;

    /**
     * @throws java.lang.Exception
     */
    @Before public void setUp() throws Exception {
        clusterConfig = new PBClusterConfig(500);
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.config.ClusterConfig#addHosts(java.lang.String[])}
     * .
     */
    @Test public void forHosts() {
        PBClientConfig config = PBClientConfig.defaults();

        clusterConfig.addHosts(HOSTS);

        List<PBClientConfig> clientConfigs = clusterConfig.getClients();
        assertEquals("Expected 3 client configs", 3, clientConfigs.size());

        List<String> clientHosts = new ArrayList<String>();

        for (PBClientConfig pbc : clusterConfig.getClients()) {
            clientHosts.add(pbc.getHost());
            configsEqual(config, pbc);
        }

        assertTrue("Expected an entry per host", clientHosts.containsAll(Arrays.asList(HOSTS)));
    }

    /**
     * Test method for
     * {@link com.basho.riak.client.raw.config.ClusterConfig#addHosts(com.basho.riak.client.raw.config.Configuration, java.lang.String[])}
     * .
     */
    @Test public void forHostsWithConfig() {
        PBClientConfig config = new PBClientConfig.Builder()
                   .withConnectionTimeoutMillis(50000)
                   .withPoolSize(10)
                   .withPort(999)
               .build();

        clusterConfig.addHosts(config, HOSTS);

        List<PBClientConfig> clientConfigs = clusterConfig.getClients();
        assertEquals("Expected 3 client configs", 3, clientConfigs.size());

        List<String> clientHosts = new ArrayList<String>();

        for (PBClientConfig pbc : clusterConfig.getClients()) {
            clientHosts.add(pbc.getHost());
            configsEqual(config, pbc);
        }

        assertTrue("Expected an entry per host", clientHosts.containsAll(Arrays.asList(HOSTS)));
    }

    private void configsEqual(PBClientConfig config, PBClientConfig config2) {
        assertEquals(config.getConnectionWaitTimeoutMillis(), config2.getConnectionWaitTimeoutMillis());
        assertEquals(config.getIdleConnectionTTLMillis(), config2.getIdleConnectionTTLMillis());
        assertEquals(config.getInitialPoolSize(), config2.getInitialPoolSize());
        assertEquals(config.getPoolSize(), config2.getPoolSize());
        assertEquals(config.getPort(), config2.getPort());
        assertEquals(config.getSocketBufferSizeKb(), config2.getSocketBufferSizeKb());
    }
}
