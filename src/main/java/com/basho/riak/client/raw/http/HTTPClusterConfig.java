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

import com.basho.riak.client.raw.config.ClusterConfig;

/**
 * Concrete child of {@link ClusterConfig} that provides the generic type
 * 
 * @author russell
 * 
 */
public class HTTPClusterConfig extends ClusterConfig<HTTPClientConfig> {

    /**
     * @param totalMaximumConnections
     */
    public HTTPClusterConfig(int totalMaximumConnections) {
        super(totalMaximumConnections);
    }

}
