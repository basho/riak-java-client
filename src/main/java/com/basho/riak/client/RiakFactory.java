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
package com.basho.riak.client;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakClientFactory;
import com.basho.riak.client.raw.config.ClusterConfig;
import com.basho.riak.client.raw.config.Configuration;
import com.basho.riak.client.raw.http.HTTPClientAdapter;
import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.http.HTTPClusterClientFactory;
import com.basho.riak.client.raw.http.HTTPClusterConfig;
import com.basho.riak.client.raw.http.HTTPRiakClientFactory;
import com.basho.riak.client.raw.pbc.PBClientAdapter;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterClientFactory;
import com.basho.riak.client.raw.pbc.PBClusterConfig;
import com.basho.riak.client.raw.pbc.PBRiakClientFactory;
import com.basho.riak.pbc.RiakClient;

/**
 * A factory for getting an IRiakClient implementation wrapping the
 * {@link RawClient} of your choice.
 * 
 * <p>
 * Use the newClient method, passing an implementation of {@link Configuration}.
 * The factory will look up a {@link RiakClientFactory} implementation and pass
 * your {@link Configuration} to it.
 * </p>
 * <p>
 * For example:
 * 
 * <code>
 * <pre>
 * Configuration conf = new PBClientConfig.Builder().withHost("my-riak-host.com").withPort(9000).build();
 * IRiakClient client = RiakFactory.newClient(conf);
 * </pre>
 * </code>
 * </p>
 * <p>
 * Also provides convenience methods for grabbing a default configuration pb or
 * http client.
 * </p>
 * 
 * @author russell
 * 
 * @see Configuration
 * @see ClusterConfig
 */
public class RiakFactory {

    private static final ConcurrentHashMap<Class<? extends Configuration>, RiakClientFactory> REGISTRY =
            new ConcurrentHashMap<Class<? extends Configuration>, RiakClientFactory>(3);

    // TODO this will change to expose a registry of
    // Configuration implementation type -> factory to complete the SPI
    // For now does a simple pre-populate for the library provided types
    static {
        REGISTRY.put(HTTPClientConfig.class, HTTPRiakClientFactory.getInstance());
        REGISTRY.put(PBClientConfig.class, PBRiakClientFactory.getInstance());
        REGISTRY.put(PBClusterConfig.class, PBClusterClientFactory.getInstance());
        REGISTRY.put(HTTPClusterConfig.class, HTTPClusterClientFactory.getInstance());
    }

    private static final String DEFAULT_RIAK_URL = "http://127.0.0.1:8098/riak";

    /**
     * Wraps a {@link PBClientAdapter} connected to 127.0.0.1:8087 in a {@link DefaultRiakClient}.
     * @return a default configuration PBC client
     * @throws RiakException
     */
    public static IRiakClient pbcClient() throws RiakException {

        try {
            final RawClient client = new PBClientAdapter("127.0.0.1", 8087);

            return new DefaultRiakClient(client);
        } catch (IOException e) {
            throw new RiakException(e);
        }
    }

    /**
     * Wraps a {@link PBClientAdapter} connected to <code>host</code> and
     * <code>port</code> in a {@link DefaultRiakClient}.
     * 
     * @return a default configuration PBC client
     * @throws RiakException
     */
    public static IRiakClient pbcClient(String host, int port) throws RiakException {

        try {
            final RawClient client = new PBClientAdapter(host, port);

            return new DefaultRiakClient(client);
        } catch (IOException e) {
            throw new RiakException(e);
        }
    }

    /**
     * Wraps the given {@link RiakClient} client in a {@link DefaultRiakClient}.
     * @param delegate the pbc.{@link RiakClient} to wrap.
     * @return a {@link DefaultRiakClient} that delegates to <code>delegate</code>
     */
    public static IRiakClient pbcClient(com.basho.riak.pbc.RiakClient delegate) {
        final RawClient client = new PBClientAdapter(delegate);
        return new DefaultRiakClient(client);
    }

    /**
     * Wraps a {@link HTTPClientAdapter} connecting to 127.0.0.1:8098/riak in a {@link DefaultRiakClient}
     * @return a default configuration {@link DefaultRiakClient} delegating to the HTTP client
     */
    public static IRiakClient httpClient() throws RiakException {
        final RawClient client = new HTTPClientAdapter(DEFAULT_RIAK_URL);
        return new DefaultRiakClient(client);
    }

    /**
     * Wraps a {@link HTTPClientAdapter} connecting to the given
     * <code>url</code> in a {@link DefaultRiakClient}
     * 
     * @param url
     *            a String of the url for Riak's REST iterface
     * @return a default configuration {@link DefaultRiakClient} delegating to
     *         the HTTP client
     */
    public static IRiakClient httpClient(String url) throws RiakException {
        final RawClient client = new HTTPClientAdapter(url);
        return new DefaultRiakClient(client);
    }

    /**
     * Wraps the given {@link com.basho.riak.client.http.RiakClient} in a {@link DefaultRiakClient}
     * @param delegate the http.{@link com.basho.riak.client.http.RiakClient} to wrap.
     * @return a {@link DefaultRiakClient} that delegates to <code>delegate</code>
     */
    public static IRiakClient httpClient(com.basho.riak.client.http.RiakClient delegate) throws RiakException {
        final RawClient client = new HTTPClientAdapter(delegate);
        return new DefaultRiakClient(client);
    }

    /**
     * Uses the given <code>config</code> to generate an {@link IRiakClient}
     * instance.
     * 
     * See the available {@link Configuration} implementations for details.
     * 
     * @param config
     *            a concrete implementation of {@link Configuration}
     * @return an {@link IRiakClient} that delegates to a {@link RawClient}
     *         configured by <code>config</code>
     * @throws IOException
     * @see HTTPClientConfig
     * @see PBClientConfig
     * @see ClusterConfig
     * @throws NoFactoryForConfigException
     *             if the {@link Configuration} type is not recognized
     * @throws IllegalArgumentException
     *             if config is null
     */
    public static IRiakClient newClient(Configuration config) throws RiakException {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }

        final RiakClientFactory fac = getFactory(config);
        try {
            return new DefaultRiakClient(fac.newClient(config));
        } catch (IOException e) {
            throw new RiakException(e);
        }
    }

    /**
     * @param config
     * @return a {@link RiakClientFactory} that will build client for the given
     *         <code>config</code>
     * @throws NoFactoryForConfigException
     *             if a factory cannot be found that accepts the
     *             {@link Configuration} type
     */
    private static RiakClientFactory getFactory(final Configuration config) {
        final RiakClientFactory fac = REGISTRY.get(config.getClass());
        if (fac == null) {
            throw new NoFactoryForConfigException(config.getClass());
        }
        return fac;
    }
}
