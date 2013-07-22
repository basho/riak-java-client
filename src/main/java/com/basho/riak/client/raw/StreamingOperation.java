/*
 * Copyright 2013 Basho Technologies Inc
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
package com.basho.riak.client.raw;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

/**
 * Encapsulates a streaming Riak operation.
 * 
 * @author Brian Roach <roach at basho dot com>
 */
public interface StreamingOperation<T> extends Iterable<T>, Iterator<T>
{
    /**
     * Retrieve all elements from the operation and return them in a List
     * 
     * @return a List containing all elements resulting from the operation
     * @throws IOException if a network error occurs 
     */
    Set<T> getAll();
    
    /**
     * Cancels the operation and cleans up the underlying network connection
     */
    void cancel();
}
