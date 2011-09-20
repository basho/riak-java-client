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

import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.cap.Quorum;

/**
 * @author russell
 *
 */
public class QuorumDeserializer extends JsonDeserializer<Quorum>{

    /* (non-Javadoc)
     * @see org.codehaus.jackson.map.JsonDeserializer#deserialize(org.codehaus.jackson.JsonParser, org.codehaus.jackson.map.DeserializationContext)
     */
    @Override public Quorum deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException,
            JsonProcessingException {
        JsonToken token = jp.getCurrentToken();
        switch (token) {
        case VALUE_STRING: {
            return new Quorum(Quora.fromString(jp.getText()));
        }
        case VALUE_NUMBER_INT: {
            return new Quorum(jp.getIntValue());
        }
        case VALUE_NULL: {
            return null;
        }
        default:
            break;
        }
        throw ctxt.mappingException(Quorum.class);
    }

}
