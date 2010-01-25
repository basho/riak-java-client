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
package com.basho.riak.client.request;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.basho.riak.client.util.Constants;

/**
 * Extra headers and query parameters to send with a Riak operation.
 */
public class RequestMeta {

    private Map<String, String> queryParams = new LinkedHashMap<String, String>();
    private Map<String, String> headers = new HashMap<String, String>();

    /**
     * Use the given r parameter for fetchMeta, fetch, or stream operations
     * 
     * @param r
     *            r- parameter for fetchMeta, fetch, or stream: the number of
     *            successful read response required for a successful overall
     *            response
     * @return A {@link RequestMeta} object with the appropriate query
     *         parameters
     */
    public static RequestMeta readParams(int r) {
        RequestMeta meta = new RequestMeta();
        meta.setQueryParam(Constants.QP_R, Integer.toString(r));
        return meta;
    }

    /**
     * Use the given w and dw params for store or delete operations.
     * 
     * @param w
     *            w- parameter for store and delete: the number of successful
     *            write responses required for a successful store operation
     * @param dw
     *            dw- parameter for store and delete: The number of successful
     *            durable write responses required for a successful store
     *            operation
     * @return A {@link RequestMeta} object with the appropriate query
     *         parameters
     */
    public static RequestMeta writeParams(Integer w, Integer dw) {
        RequestMeta meta = new RequestMeta();
        if (w != null) {
            meta.setQueryParam(Constants.QP_W, Integer.toString(w));
        }
        if (dw != null) {
            meta.setQueryParam(Constants.QP_DW, Integer.toString(dw));
        }
        return meta;
    }

    /**
     * Add the specified HTTP header
     * 
     * @param key
     *            header name
     * @param value
     *            header value
     */
    public RequestMeta setHeader(String key, String value) {
        headers.put(key, value);
        return this;
    }

    /**
     * Return the value for the HTTP header or null if not set
     * 
     * @param key
     *            header name
     * @return value of header or null if not set
     */
    public String getHeader(String key) {
        return headers.get(key);
    }

    /**
     * @param key
     *            header name
     * @return whether the HTTP header has been set
     */
    public boolean hasHeader(String key) {
        return headers.containsKey(key);
    }

    /**
     * @return Map of HTTP header names to values
     */
    public Map<String, String> getHeaders() {
        return headers;
    }

    /**
     * @param param
     *            query parameter name
     * @return query parameter value or null if not set
     */
    public String getQueryParam(String param) {
        return queryParams.get(param);
    }

    /**
     * Add the given query parameter to the request
     * 
     * @param param
     *            query parameter name
     * @param value
     *            query parameter value
     */
    public RequestMeta setQueryParam(String param, String value) {
        queryParams.put(param, value);
        return this;
    }

    /**
     * @return A string containing all the specified query parameters in this
     *         {@link RequestMeta} in the form: p1=v1&p2=v2
     */
    public String getQueryParams() {
        StringBuilder qp = new StringBuilder();
        for (String param : queryParams.keySet()) {
            if (queryParams.get(param) != null) {
                if (qp.length() > 0) {
                    qp.append("&");
                }
                qp.append(param).append("=").append(queryParams.get(param));
            }
        }
        return qp.toString();
    }
}
