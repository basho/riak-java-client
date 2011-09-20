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
package com.basho.riak.client.raw.http;

import java.io.IOException;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;

import com.basho.riak.client.http.util.Constants;
import com.basho.riak.client.query.functions.NamedJSFunction;

/**
 * @author russell
 * 
 */
public class NamedJSFunctionDeserializer extends JsonDeserializer<NamedJSFunction> {
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.codehaus.jackson.map.JsonDeserializer#deserialize(org.codehaus.jackson
     * .JsonParser, org.codehaus.jackson.map.DeserializationContext)
     */
    @Override public NamedJSFunction deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException,
            JsonProcessingException {

        JsonToken token = jp.getCurrentToken();

        if (JsonToken.START_OBJECT.equals(token)) {

            String name = null;

            while (!JsonToken.END_OBJECT.equals(token)) {
                String field = jp.getCurrentName();

                if (Constants.FL_SCHEMA_FUN_NAME.equals(field)) {
                    jp.nextToken();
                    name = jp.getText();
                }
                token = jp.nextToken();
            }
            if (name != null) {
                return new NamedJSFunction(name);
            } else {
                return null;
            }
        }
        throw ctxt.mappingException(NamedJSFunction.class);
    }

}
