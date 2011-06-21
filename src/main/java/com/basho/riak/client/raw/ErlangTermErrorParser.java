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

import com.basho.riak.client.RiakException;
import com.basho.riak.client.raw.query.MapReduceTimeoutException;

/**
 * Riak's PB interface returns errors as erlang terms encoded as strings, this
 * parses them into the most specific checked exception
 * 
 * @author russell
 * 
 */
public class ErlangTermErrorParser {

    private static final String TIMEOUT_TUPLE = "{error,timeout}";

    public static RiakException parseErlangError(final String erlangError) {
        // TODO, ok, this is lame, but it does the job at the moment
        // revisit when more checked exceptions are needed
        if (TIMEOUT_TUPLE.equals(erlangError)) {
            return new MapReduceTimeoutException();
        } else {
            return new RiakException(erlangError);
        }
    }
}
