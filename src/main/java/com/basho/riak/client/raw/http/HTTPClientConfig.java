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

import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpRequestRetryHandler;

import com.basho.riak.client.raw.config.Configuration;

/**
 * The set of configuration parameters to use when creating an HTTP RawClient
 * instance
 * 
 * @author russell
 * 
 */
public class HTTPClientConfig implements Configuration {

    private final String url;
    private final String mapreducePath;
    private final HttpClient httpClient;
    private final Integer timeout;
    private final Integer maxConnections;
    private final HttpRequestRetryHandler retryHandler;

    /**
     * Create a new instance, use the {@link Builder}
     * 
     * @param url
     *            the url for Riak's REST interface (scheme://host:port/path)
     * @param mapreducePath
     *            the path to Riak's REST M/R interfa ce (eg /mapreduce)
     * @param httpClient
     *            a fully configured Apache {@link HttpClient} that you want to
     *            be used by Riak Http client
     * @param timeout
     *            the connection and socket read timeout in milliseconds
     * @param maxConnections
     *            the maximum number of connections to Riak to create
     * @param retryHandler
     *            an implementation of {@link HttpRequestRetryHandler} to be
     *            used by the underlying {@link HttpClient}
     */
    private HTTPClientConfig(String url, String mapreducePath, HttpClient httpClient, Integer timeout,
            Integer maxConnections, HttpRequestRetryHandler retryHandler) {
        this.url = url;
        this.mapreducePath = mapreducePath;
        this.httpClient = httpClient;
        this.timeout = timeout;
        this.maxConnections = maxConnections;
        this.retryHandler = retryHandler;
    }

    /**
     * Create a config with all the default values, see {@link Builder} for the
     * defaults.
     * 
     * @return an http client config populated with default values;
     * @see Builder
     */
    public static HTTPClientConfig defaults() {
        return new Builder().build();
    }

    /**
     * @return the url
     */
    public String getUrl() {
        return url;
    }

    /**
     * @return the mapreducePath
     */
    public String getMapreducePath() {
        return mapreducePath;
    }

    /**
     * @return the httpClient
     */
    public HttpClient getHttpClient() {
        return httpClient;
    }

    /**
     * @return the timeout
     */
    public Integer getTimeout() {
        return timeout;
    }

    /**
     * @return the maxConnections
     */
    public Integer getMaxConnections() {
        return maxConnections;
    }

    /**
     * @return the retryHandler
     */
    public HttpRequestRetryHandler getRetryHandler() {
        return retryHandler;
    }

    /**
     * Use the builder to create a new instance of {@link HTTPClientConfig}.
     * 
     * The defaults are as follows:
     * 
     * <table>
     * <tr>
     * <th>field</th>
     * <th>default value</th>
     * </tr>
     * <tr>
     * <td>url</td>
     * <td>http://127.0.0.1:8098/riak (derived from defaults for
     * scheme://host:port/path)</td>
     * </tr>
     * <tr>
     * <td>scheme</td>
     * <td>http</td>
     * </tr>
     * <tr>
     * <td>host</td>
     * <td>127.0.0.1</td>
     * </tr>
     * <tr>
     * <td>port</td>
     * <td>8098</td>
     * </tr>
     * <tr>
     * <td>path</td>
     * <td>riak</td>
     * </tr>
     * <tr>
     * <td>mapreducePath</td>
     * <td>/mapreduce</td>
     * </tr>
     * <tr>
     * <td>httpClient</td>
     * <td>null (IE the library creates one)</td>
     * </tr>
     * <tr>
     * <td>timeout</td>
     * <td>null (will then use HttpClient default which is 0 (for infinite))</td>
     * </tr>
     * <tr>
     * <td>maxConnections</td>
     * <td>null (will then use the HttpClient default which is a max of *2* per
     * route)</td>
     * </tr>
     * <tr>
     * <td>httpRequestRetryHandler</td>
     * <td>null (will use the HttpClient default)</td>
     * </tr>
     * </table>
     * 
     * @author russell
     * 
     */
    public static final class Builder {

        private String url = null;
        private String scheme = "http";
        private String host = "127.0.0.1";
        private int port = 8097;
        private String riakPath = "/riak";
        private String mapreducePath = "/mapreduce";
        private HttpClient httpClient = null;
        private Integer timeout = null;
        private Integer maxConnections = null;
        private HttpRequestRetryHandler retryHandler = null;

        public HTTPClientConfig build() {
            if (url == null) {
                StringBuilder sb = new StringBuilder(scheme).append("://").append(host).append(":").append(port);

                if (!riakPath.startsWith("/")) {
                    sb.append("/");
                }

                url = sb.append(riakPath).toString();
            }
            return new HTTPClientConfig(url, mapreducePath, httpClient, timeout, maxConnections, retryHandler);
        }

        /**
         * Create a new builder with values all copied from
         * <code>copyConfig</code>
         * 
         * @param copyConfig
         *            the {@link HTTPClientConfig} to copy values from
         * @return a new {@link Builder} populated with <code>copyConfig</code>
         *         's values.
         */
        public static Builder from(HTTPClientConfig copyConfig) {
            Builder b = new Builder();
            b.url = copyConfig.url;
            b.mapreducePath = copyConfig.mapreducePath;
            b.httpClient = copyConfig.httpClient;
            b.timeout = copyConfig.timeout;
            b.maxConnections = copyConfig.maxConnections;
            b.retryHandler = copyConfig.retryHandler;
            return b;
        }

        public Builder withUrl(String url) {
            this.url = url;
            return this;
        }

        public Builder withScheme(String scheme) {
            this.scheme = scheme;
            return this;
        }

        public Builder withHost(String host) {
            this.host = host;
            return this;
        }

        public Builder withPort(int port) {
            this.port = port;
            return this;
        }

        public Builder withRiakPath(String path) {
            this.riakPath = path;
            return this;
        }

        public Builder withMapreducePath(String path) {
            this.mapreducePath = path;
            return this;
        }

        public Builder withHttpClient(HttpClient client) {
            this.httpClient = client;
            return this;
        }

        public Builder withTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder withMaxConnctions(int maxConnections) {
            this.maxConnections = maxConnections;
            return this;
        }

        public Builder withRetryHandler(HttpRequestRetryHandler retryHandler) {
            this.retryHandler = retryHandler;
            return this;
        }
    }
}
