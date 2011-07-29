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

import com.basho.riak.client.http.RiakClient;
import com.basho.riak.client.http.RiakConfig;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakClientFactory;
import com.basho.riak.client.raw.config.Configuration;

/**
 * Factory for creating {@link HTTPClientAdapter} instances configured by the
 * given {@link HTTPClientConfig}. Converts an {@link HTTPClientConfig} to a
 * {@link RiakConfig}
 * 
 * @author russell
 * 
 */
public class HTTPRiakClientFactory implements RiakClientFactory {

    private static final HTTPRiakClientFactory instance = new HTTPRiakClientFactory();

    private HTTPRiakClientFactory() {}

    public static HTTPRiakClientFactory getInstance() {
        return instance;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RiakClientFactory#accepts(java.lang.Class)
     */
    public boolean accepts(Class<? extends Configuration> configClass) {
        if (configClass == null) {
            return false;
        }
        if (configClass.equals(HTTPClientConfig.class)) {
            return true;
        }
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RiakClientFactory#newClient(com.basho.riak.
     * client.raw.config.Configuration)
     */
    public RawClient newClient(Configuration conf) {
        if (conf == null) {
            throw new IllegalArgumentException("conf cannot be null");
        }
        HTTPClientConfig config = (HTTPClientConfig) conf;
        RiakConfig riakConfig = new RiakConfig();
        riakConfig.setUrl(config.getUrl());
        riakConfig.setHttpClient(config.getHttpClient());
        riakConfig.setMapReducePath(config.getMapreducePath());
        riakConfig.setMaxConnections(config.getMaxConnections());
        riakConfig.setTimeout(config.getTimeout());
        riakConfig.setRetryHandler(config.getRetryHandler());

        return new HTTPClientAdapter(new RiakClient(riakConfig));
    }
}
