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

import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakClientFactory;
import com.basho.riak.client.raw.config.Configuration;

/**
 * @author russell
 * 
 */
public class PBClusterClientFactory implements RiakClientFactory {

    private static final PBClusterClientFactory instance = new PBClusterClientFactory();

    private PBClusterClientFactory() {}

    public static PBClusterClientFactory getInstance() {
        return instance;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RiakClientFactory#accepts(java.lang.Class)
     */
    public boolean accepts(Class<? extends Configuration> configClass) {
        if (configClass.equals(PBClusterConfig.class)) {
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
    public RawClient newClient(Configuration config) throws IOException {
        PBClusterConfig conf = (PBClusterConfig) config;
        return new PBClusterClient(conf);
    }

}
