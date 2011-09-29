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
package com.basho.riak.client.bucket;

import com.basho.riak.client.raw.Transport;

/**
 * @author russell
 * 
 */
@SuppressWarnings("serial") public class UnsupportedPropertyException extends UnsupportedOperationException {

    private final Transport transport;
    private final String property;

    /**
     * @param transport
     * @param property
     */
    public UnsupportedPropertyException(Transport transport, String property) {
        super(property + " not supported for " + transport);
        this.transport = transport;
        this.property = property;
    }

    /**
     * @return the transport
     */
    public synchronized Transport getTransport() {
        return transport;
    }

    /**
     * @return the property
     */
    public synchronized String getProperty() {
        return property;
    }

}
