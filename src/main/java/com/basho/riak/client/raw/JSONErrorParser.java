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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

import com.basho.riak.client.RiakException;
import com.basho.riak.client.raw.query.MapReduceTimeoutException;

/**
 * Exceptions come back from Riak as JSON, parses exceptions, and throws the
 * most specific exception it can.
 * 
 * @author russell
 * 
 */
public class JSONErrorParser {
    private static final String PBSOCKET_PRE_1_1_TIMEOUT_TUPLE = "{error,timeout}";
    private static final String PARSE_ERROR = ". Additionally, an error was thrown parsing the error response.";
    private static final String ERROR_KEY = "error";
    private static final String TIMEOUT_VALUE = "timeout";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses some Riak error <code>json</code> into a checked exception of the
     * most specific type it can manage.
     * 
     * @param json
     *            The error from Riak
     * @return a {@link RiakException} as specific as possible
     * @throws IOException
     */
    public static RiakException parseException(final String json) {
        RiakException ex = null;
        try {
            Map<String, String> exceptionData = parseError(json);

            if (isTimeoutError(exceptionData)) {
                ex = new MapReduceTimeoutException();
            } else {
                ex = new RiakException(json);
            }
        } catch (IOException e) {
            ex = new RiakException(new StringBuilder().append(json).append(PARSE_ERROR).toString(), e);
        }

        return ex;
    }

    /**
     * Does the <code>json</code> represent a riak map/reduce timeout exception?
     * 
     * @param json
     *            an error String from Riak
     * @return true if <code>json</code> represents an m/r timeout, false
     *         otherwse
     * @throws IOException
     */
    public static boolean isTimeoutException(final String json) {

        try {
            Map<String, String> exceptionData = parseError(json);
            return isTimeoutError(exceptionData);
        } catch (IOException e) {
            // what about an old skool Erlang term exception from the PB Socket?
            // fall back to the string match on 'timeout'
            // TODO: @remove:1.2
            if (PBSOCKET_PRE_1_1_TIMEOUT_TUPLE.equals(json)) {
                return true;
            }
            // can't be a timeout if we can't parse it
            return false;
        }
    }

    /**
     * Internal parse method, just calls ObjectMapper to get a map from the
     * string
     * 
     * @param json
     *            a JSON String
     * @return the Map representation of the JSON
     * @throws IOException
     */
    private static final Map<String, String> parseError(final String json) throws IOException {
        return objectMapper.readValue(json, objectMapper.getTypeFactory().constructMapType(HashMap.class, String.class, String.class));
    }

    /**
     * Does the map of exception data represent an m/r timeout exception?
     * 
     * @param exceptionData
     * @return true if <code>exceptionData</code> contains key "error" with
     *         value "timeout". False otherwise.
     */
    private static boolean isTimeoutError(final Map<String, String> exceptionData) {
        return (exceptionData.containsKey(ERROR_KEY) && TIMEOUT_VALUE.equals(exceptionData.get(ERROR_KEY)));
    }

}
