/*
 * This file is provided to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain
 * a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.basho.riak.client;

import org.apache.commons.httpclient.HttpClient;

public class RiakConfig {

    public RiakConfig() {}

    public RiakConfig(String url) {
        this.setUrl(url);
    }

    public RiakConfig(String ip, String port, String prefix) {
        if (prefix == null) {
            prefix = "";
        }
        this.setUrl("http://" + ip + ":" + port + prefix);
    }

    private String url = null;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    private HttpClient httpClient = null;

    public HttpClient getHttpClient() {
        return httpClient;
    }

    public void setHttpClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(final long timeout) {
        this.timeout = timeout;
    }

    private Long timeout = null;

    public Integer getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(Integer maxConnections) {
        this.maxConnections = maxConnections;
    }

    private Integer maxConnections = null;
}
