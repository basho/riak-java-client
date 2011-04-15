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
package com.basho.riak.newapi;

import java.io.IOException;

import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.http.HTTPClientAdapter;
import com.basho.riak.client.raw.pbc.PBClientAdapter;

/**
 * @author russell
 * 
 */
public class RiakFactory {

    private static final String DEFAULT_RIAK_URL = "http://127.0.0.1:8098/riak";

    /**
     * 
     * @return a default configuration PBC client
     * @throws RiakException
     */
    public static RiakClient pbcClient() throws RiakException {

        try {
            final RawClient client = new PBClientAdapter("127.0.0.1", 8087);

            return new DefaultClient(client);
        } catch (IOException e) {
            throw new RiakException(e);
        }
    }

    /**
     * @return a default configuration HTTP client
     */
    public static RiakClient httpClient() throws RiakException {
        final RawClient client = new HTTPClientAdapter(DEFAULT_RIAK_URL);
        return new DefaultClient(client);
    }

    /**
     * @return a wrapped RiakClient
     */
    public static RiakClient httpClient(com.basho.riak.client.RiakClient delegate) throws RiakException {
        final RawClient client = new HTTPClientAdapter(delegate);
        return new DefaultClient(client);
    }

}
