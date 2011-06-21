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
package com.basho.riak.client.cap;

import java.util.concurrent.Callable;

import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.operations.RiakOperation;

/**
 * Fault tolerant systems need fault tolerant clients, implement to retry failed operations.
 * 
 * @author russell
 */
public interface Retrier {
    /**
     * Called by {@link RiakOperation} execute methods to give some measure of fault tolerance.
     * 
     * @param <T> the Type the {@link Callable#call()} returns
     * @param command the {@link Callable}
     * @return the value of {@link Callable#call()}
     * @throws RiakRetryFailedException if the retrier exceeds its bounds.
     */
    <T> T attempt(Callable<T> command) throws RiakRetryFailedException;
}
