/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.client;

import org.apache.commons.httpclient.HttpClient;

/**
 * Configuration settings for connecting to a Riak instance including the URL
 * and settings for HttpClient. A pre-constructed HttpClient can also be
 * provided.
 */
public class RiakConfig {

    private String url = null;
    private HttpClient httpClient = null;
    private Long timeout = null;
    private Integer maxConnections = null;

    public RiakConfig() {}

    public RiakConfig(String url) {
        if (url == null || url.length() == 0)
            throw new IllegalArgumentException();

        this.setUrl(url);
    }

    public RiakConfig(String ip, String port, String prefix) {
        if (prefix == null) {
            prefix = "";
        }

        this.setUrl("http://" + ip + ":" + port + prefix);
    }

    /**
     * The base URL used by a client to construct object URLs
     */
    public String getUrl() {
        return url;
    }

    /**
     * Set the base URL that clients should use to construct object URLs (eg
     * http://localhost:8098/jiak).
     */
    public void setUrl(String url) {
        this.url = url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    /**
     * The pre-constructed HttpClient for a client to use if one was provided
     */
    public HttpClient getHttpClient() {
        return httpClient;
    }

    /**
     * Provide a pre-constructed HttpClient for clients to use to connect to
     * Riak
     */
    public void setHttpClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    /**
     * Value to set for the HttpClientParams.CONNECTION_MANAGER_TIMEOUT
     * property: timeout in milliseconds for retrieving an HTTP connection. Null
     * for default.
     */
    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(final long timeout) {
        this.timeout = timeout;
    }

    /**
     * Value to set for the HttpConnectionManagerParams.MAX_TOTAL_CONNECTIONS
     * property: overall maximum number of connections used by the HttpClient.
     */
    public Integer getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(Integer maxConnections) {
        this.maxConnections = maxConnections;
    }
}
