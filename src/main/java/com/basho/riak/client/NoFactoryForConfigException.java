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

import com.basho.riak.client.raw.RiakClientFactory;
import com.basho.riak.client.raw.config.Configuration;

/**
 * {@link RuntimeException} thrown by {@link RiakFactory} if it is unable to
 * find a {@link RiakClientFactory} to create a client for the given
 * configuration.
 * 
 * @author russell
 * 
 */
public class NoFactoryForConfigException extends RuntimeException {

    /**
     * Eclipse generated.
     */
    private static final long serialVersionUID = -2159863405694749013L;
    private final Class<? extends Configuration> configType;

    /**
     * @param e
     * @param configType
     */
    public NoFactoryForConfigException(Class<? extends Configuration> configType) {
        super();
        this.configType = configType;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Throwable#getMessage()
     */
    @Override public String getMessage() {
        return "Unable to find a factory for " + configType.getName();
    }

    /**
     * @return the configType
     */
    public Class<? extends Configuration> getConfigType() {
        return configType;
    }
}
