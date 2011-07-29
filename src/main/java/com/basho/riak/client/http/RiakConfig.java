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
package com.basho.riak.client.http;

import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpRequestRetryHandler;

/**
 * Configuration settings for connecting to a Riak instance such as the base
 * Riak URL and HttpClient settings. A pre-constructed HttpClient can also be
 * provided.
 */
public class RiakConfig {

    public static Pattern BASE_URL_PATTERN = Pattern.compile("^((?:[^:]*://)?[^/]*)");

    private String url = null;
    private String baseUrl = null;
    private String mapredPath = "/mapred";
    private String pingPath = "/ping";
    private HttpClient httpClient = null;
    private Integer timeout = null;
    private Integer maxConnections = null;
    private HttpRequestRetryHandler retryHandler = null;

    public RiakConfig() {}

    public RiakConfig(String url) {
        if (url == null || url.length() == 0)
            throw new IllegalArgumentException();

        this.setUrl(url);
    }
    
    public RiakConfig(URL url) {
       if (url == null) {
          throw new IllegalArgumentException();
       }
       
       String protocol = url.getProtocol().toLowerCase();
       if(!protocol.equals("http") && !protocol.equals("https")) {
          throw new IllegalArgumentException();
       }
       
       this.setUrl(url.toExternalForm());
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
     * Set the base URL that clients should use to construct object URLs (e.g.
     * http://localhost:8098/riak).
     */
    public void setUrl(String url) {
        this.url = url.endsWith("/") ? url.substring(0, url.length() - 1) : url;

        Matcher m = BASE_URL_PATTERN.matcher(url);
        if (m.find()) {
            baseUrl = m.group();
        } else {
            baseUrl = this.url;
        }
    }

    /**
     * The full URL of Riak map reduce resource, which is calculated by
     * combining the host and port from the Riak URL and the map reduce path.
     */
    public String getMapReduceUrl() {
        return baseUrl + mapredPath;
    }

    /**
     * The full URL of the Riak ping resource, which is calculated by
     * combining the host and port from the Riak URL and the ping path.
     */
    public String getPingUrl() {
        return baseUrl + pingPath;
    }

    /**
     * The host and port of the Riak server, which is extracted from the
     * specified Riak URL.
     */
    public String getBaseUrl() {
        return baseUrl;
    }

    /**
     * The path to the Riak map reduce resource, which defaults to /mapred
     */
    public String getMapReducePath() {
        return mapredPath;
    }

    public void setMapReducePath(String path) {
        mapredPath = cleanUpPath(path);
    }

    public String getPingPath() {
        return pingPath;
    }

    public void setPingPath(String path) {
        pingPath = cleanUpPath(path);
    }

    private String cleanUpPath(String path) {
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        return path;
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
     * Value to set for the properties:
     * {@link HttpClientParams#CONNECTION_MANAGER_TIMEOUT},
     * {@link HttpClientParams#SO_TIMEOUT},
     * {@link HttpConnectionManagerParams#CONNECTION_TIMEOUT} which sets the
     * timeout milliseconds for retrieving an HTTP connection and data over the
     * connection. Null for default.
     */
    public void setTimeout(final Integer timeout) {
        this.timeout = timeout;
    }

    public Integer getTimeout() {
        return timeout;
    }

    /**
     * Value to set for the HttpConnectionManagerParams.MAX_TOTAL_CONNECTIONS
     * and the HttpConnectionManagerParams.MAX_HOST_CONNECTIONS properties: 
     * overall maximum number of connections used by the HttpClient and the
     * maximum number of connections per host. The latter is set to overcome
     * the default 2 connections per host in HttpClient.
     */
    public void setMaxConnections(Integer maxConnections) {
        this.maxConnections = maxConnections;
    }

    public Integer getMaxConnections() {
        return maxConnections;
    }

    /**
     * Get value set for the http client retry handler, the default retry
     * handler for requests.
     * 
     * @see org.apache.http.client.HttpRequestRetryHandler
     * @see org.apache.http.impl.client.DefaultHttpRequestRetryHandler
     */
    public HttpRequestRetryHandler getRetryHandler() {
        return retryHandler;
    }

    /**
     * Value to set for the http client retry handler, the default retry handler
     * for requests.
     * 
     * @see org.apache.http.client.HttpRequestRetryHandler
     * @see org.apache.http.impl.client.DefaultHttpRequestRetryHandler
     */
    public void setRetryHandler(HttpRequestRetryHandler retryHandler) {
        this.retryHandler = retryHandler;
    }
}
