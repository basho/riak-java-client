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
package com.basho.riak.client.query.serialize;

import com.basho.riak.client.query.functions.Function;
import com.basho.riak.client.query.functions.JSBucketKeyFunction;
import com.basho.riak.client.query.functions.JSSourceFunction;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import com.basho.riak.client.query.functions.NamedJSFunction;
import com.fasterxml.jackson.core.JsonGenerator;

/**
 * Helper to write a Function to a JsonGenerator
 * 
 * @author russell
 * 
 */
public class FunctionToJson {

    public static FunctionWriter newWriter(Function function, JsonGenerator jsonGenerator) {
        // eugh
        if (function instanceof NamedErlangFunction) {
            return new NamedErlangFunctionWriter((NamedErlangFunction) function, jsonGenerator);
        } else if (function instanceof NamedJSFunction) {
            return new NamedJSFunctionWriter((NamedJSFunction) function, jsonGenerator);
        } else if (function instanceof JSSourceFunction) {
            return new JSSourceFunctionWriter((JSSourceFunction) function, jsonGenerator);
        } else if (function instanceof JSBucketKeyFunction) {
            return new JSBucketKeyFunctionWriter((JSBucketKeyFunction) function, jsonGenerator);
        }

        throw new IllegalArgumentException("No writer for function type " + function.getClass());
    }
}
