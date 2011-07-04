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
package com.basho.riak.pbc;

import java.io.IOException;

/**
 * Thrown by the {@link RiakConnectionPool} if acquire times out.
 * 
 * @author russell
 * 
 */
public class AcquireConnectionTimeoutException extends IOException {

    /**
     * 
     */
    private static final long serialVersionUID = 7603281140234757892L;

    /**
     * 
     */
    public AcquireConnectionTimeoutException() {}

    /**
     * @param message
     */
    public AcquireConnectionTimeoutException(String message) {
        super(message);
    }
}
