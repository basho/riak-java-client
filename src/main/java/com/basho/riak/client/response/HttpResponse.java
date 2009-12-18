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
package com.basho.riak.client.response;

import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;

/**
 * HTTP response information resulting from some HTTP operation
 */
public interface HttpResponse {

    /**
     * @return The target object's bucket
     */
    public String getBucket();

    /**
     * @return The target object's key or null if bucket is target
     */
    public String getKey();

    /**
     * @return Resulting status code from the HTTP request.
     */
    public int getStatusCode();

    /**
     * @return The HTTP response headers.
     */
    public Map<String, String> getHttpHeaders();

    /**
     * @return The HTTP response body.
     */
    public String getBody();

    /**
     * @return The actual {@link HttpMethod} used to make the HTTP request. Most
     *         of the data here can be retrieved more simply using methods in
     *         this class. Also, note that the connection will already be
     *         closed, so calling getHttpMethod().getResponseBodyAsStream() will
     *         return null.
     */
    public HttpMethod getHttpMethod();

    /**
     * @return Whether the HTTP request returned a 2xx or 404 in case of DELETE
     *         response
     */
    public boolean isSuccess();

    /**
     * @return Whether the HTTP request returned a 4xx or 5xx response
     */
    public boolean isError();

}
