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

import java.io.IOException;


import com.basho.riak.client.query.functions.JSBucketKeyFunction;
import com.fasterxml.jackson.core.JsonGenerator;

/**
 * Writes a {@link JSBucketKeyFunction} to a {@link JsonGenerator}
 * @author russell
 *
 */
public class JSBucketKeyFunctionWriter implements FunctionWriter {
    
    private final JSBucketKeyFunction function;
    private final JsonGenerator jsonGenerator;

    /**
     * @param function
     * @param jsonGenerator
     */
    public JSBucketKeyFunctionWriter(JSBucketKeyFunction function, JsonGenerator jsonGenerator) {
        this.function = function;
        this.jsonGenerator = jsonGenerator;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.newapi.query.serialize.FunctionWriter#write()
     */
    public void write() throws IOException {
        jsonGenerator.writeStringField("language", "javascript");
        jsonGenerator.writeStringField("bucket", function.getBucket());
        jsonGenerator.writeStringField("key", function.getKey());
    }

}
