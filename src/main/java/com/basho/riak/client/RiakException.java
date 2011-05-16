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

/**
 * @author russell
 * 
 */
public class RiakException extends Exception {

    /**
     * 
     */
    private static final long serialVersionUID = 7644302774003494842L;

    /**
     * @param e
     */
    public RiakException(Throwable e) {
        super(e);
    }

    public RiakException() {
        super();
    }

    public RiakException(String message) {
        super(message);
    }

    /**
     * @param message String
     * @param cause
     */
    public RiakException(String message, Throwable cause) {
        super(message, cause);
    }
}
