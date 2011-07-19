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
package com.basho.riak.client.raw;

import java.io.IOException;

import com.basho.riak.client.raw.config.Configuration;

/**
 * Top level interface for {@link RawClient} factories
 * 
 * @author russell
 * 
 * @param <T>
 *            the {@link Configuration} type the factory will build for.
 */
public interface RiakClientFactory {

    /**
     * Is the concrete factory able to create {@link RawClient}s with
     * <code>configClass</code> {@link Configuration}s?
     * 
     * *ALWAYS* call before newClient.
     * 
     * @param configClass
     *            the {@link Configuration} implementation
     * @return true if this factory can create a {@link RawClient} for the
     *         <code>configClass</code> false otherwise
     */
    public boolean accepts(Class<? extends Configuration> configClass);

    /**
     * Create a new {@link RawClient} instance configured by <code>config</code>
     * 
     * @param config
     *            a specific implementation of {@link Configuration}
     * @return the configured RawClient
     */
    public RawClient newClient(Configuration config) throws IOException;
}
