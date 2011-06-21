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

import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.http.HTTPClientAdapter;
import com.basho.riak.client.raw.pbc.PBClientAdapter;
import com.basho.riak.pbc.RiakClient;

/**
 * A *very* basic factory for getting an IRiakClient implementation wrapping
 * the {@link RawClient} of your choice.
 * <p>
 * Also provides convenience methods for grabbing a default configuration pb or http client.
 * </p>
 * <p>
 * NOTE: This class is under change, a single factory method that accepts a <code>Configuration</code> object will
 * be available soon
 * </p>
 * @author russell 
 */
public class RiakFactory {

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

}
