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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import com.basho.riak.client.http.util.Constants;
import com.basho.riak.client.query.functions.NamedErlangFunction;

/**
 * Deserializes {@link NamedErlangFunction}s from JSON
 * 
 * @author russell
 * 
 */
public class NamedErlangFunctionDeserializer extends JsonDeserializer<NamedErlangFunction> {
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.codehaus.jackson.map.JsonDeserializer#deserialize(org.codehaus.jackson
     * .JsonParser, org.codehaus.jackson.map.DeserializationContext)
     */
    @Override public NamedErlangFunction deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException,
            JsonProcessingException {

        JsonToken token = jp.getCurrentToken();

        if (JsonToken.START_OBJECT.equals(token)) {

            String mod = null;
            String fun = null;

            while (!JsonToken.END_OBJECT.equals(token)) {
                String field = jp.getCurrentName();

                if (Constants.FL_SCHEMA_FUN_MOD.equals(field)) {
                    jp.nextToken();
                    mod = jp.getText();
                } else if (Constants.FL_SCHEMA_FUN_FUN.equals(field)) {
                    jp.nextToken();
                    fun = jp.getText();
                }
                token = jp.nextToken();
            }
            if (mod != null && fun != null) {
                return new NamedErlangFunction(mod, fun);
            } else {
                return null;
            }
        }
        throw ctxt.mappingException(NamedErlangFunction.class);
    }

}
