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
package com.basho.riak.client.raw.http;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpHost;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;

import com.basho.riak.client.http.RiakClient;
import com.basho.riak.client.http.RiakConfig;
import com.basho.riak.client.raw.ClusterClient;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.config.ClusterConfig;

/**
 * Cluster client that creates a collection of {@link HTTPClientAdapter}
 * {@link RawClient} instances from a given {@link HTTPClusterConfig}
 * <p>
 * NOTE: to enforce the max connections creates the {@link HttpClient} delegates
 * for the individual node clients, this means that the
 * {@link HTTPClientConfig#getHttpClient()} values is ignored.
 * </p>
 * 
 * @author russell
 * 
 */
public class HTTPClusterClient extends ClusterClient<HTTPClientConfig> {

    /**
     * @param clusterConfig
     * @throws IOException
     */
    public HTTPClusterClient(ClusterConfig<HTTPClientConfig> clusterConfig) throws IOException {
        super(clusterConfig);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.ClusterClient#fromConfig(com.basho.riak.client
     * .raw.config.ClusterConfig)
     */
    @Override protected RawClient[] fromConfig(ClusterConfig<HTTPClientConfig> clusterConfig) throws IOException {
        List<RawClient> clients = new ArrayList<RawClient>();
        int maxTotal = clusterConfig.getTotalMaximumConnections();

        // IE limitless
        if (maxTotal == ClusterConfig.UNLIMITED_CONNECTIONS) {
            // independent pools, independent clients
            HTTPRiakClientFactory fac = HTTPRiakClientFactory.getInstance();
            for (HTTPClientConfig node : clusterConfig.getClients()) {
                clients.add(fac.newClient(node));
            }
        } else {
            // create a ThreadSafeClientConnManager to be shared by all the
            // RiakClient instances
            // in the cluster
            // add a route per host and a max per route
            ThreadSafeClientConnManager cm = new ThreadSafeClientConnManager();
            cm.setMaxTotal(maxTotal);

            for (HTTPClientConfig node : clusterConfig.getClients()) {
                if (node.getMaxConnections() != null) {
                    cm.setMaxForRoute(makeRoute(node.getUrl()), node.getMaxConnections());
                }
                DefaultHttpClient httpClient = new DefaultHttpClient(cm);

                if (node.getRetryHandler() != null) {
                    httpClient.setHttpRequestRetryHandler(node.getRetryHandler());
                }

                RiakConfig riakConfig = new RiakConfig(node.getUrl());
                riakConfig.setMapReducePath(node.getMapreducePath());
                riakConfig.setTimeout(node.getTimeout());
                riakConfig.setHttpClient(httpClient);

                clients.add(new HTTPClientAdapter(new RiakClient(riakConfig)));
            }
        }
        return clients.toArray(new RawClient[clients.size()]);
    }

    /**
     * Make an {@link HttpRoute} for the given URL
     * 
     * @param url
     * @return a {@link HttpRoute} for the given URL
     */
    private HttpRoute makeRoute(String url) throws IOException {
        try {
            URI uri = new URI(url);
            HttpHost host = new HttpHost(uri.getHost(), uri.getPort());
            return new HttpRoute(host);
        } catch (URISyntaxException e) {
            throw new IOException(e.toString());
        }
    }
}
