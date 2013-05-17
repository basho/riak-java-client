/*
 * Copyright 2013 Basho Technologies Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.query;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

/**
 * A Future for an individual fetch done as part of a MultiFetch.
 * 
 * The {@link #getKey() } method is provided in the case where an object
 * is not found; {@link #get() } will return {@code null} and this method allows
 * the user to know which key was not present in Riak. 
 * 
 * @see {@link MultiFetchObject}
 * 
 * @author Brian Roach <roach at basho dot com>
 */
public class MultiFetchFuture<V> extends FutureTask<V>
{
    private final String key;
        
    public MultiFetchFuture(String key, Callable<V> callable)
    {
        super(callable);
        this.key = key;
    }

    /**
     * 
     * @return The key used for this fetch operation.
     */
    public String getKey()
    {
        return key;
    }
}
