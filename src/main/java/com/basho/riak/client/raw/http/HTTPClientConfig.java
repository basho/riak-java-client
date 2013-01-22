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

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpRequestRetryHandler;

import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.config.Configuration;

/**
 * The set of configuration parameters to use when creating an HTTP RawClient
 * instance.
 * 
 * @author russell
 * @see HTTPClusterConfig
 */
public class HTTPClientConfig implements Configuration {
	private final URI uri;
	private final String mapreducePath;
	private final HttpClient httpClient;
	private final Integer timeout;
	private final Integer maxConnections;
	private final HttpRequestRetryHandler retryHandler;

	/**
	 * Create a new instance, use the {@link Builder}
	 * 
	 * @param url
	 *            the URL for Riak's REST interface (scheme://host:port/path)
	 * @param mapreducePath
	 *            the path to Riak's REST M/R interface (eg /mapreduce)
	 * @param httpClient
	 *            a fully configured Apache {@link HttpClient} that you want to
	 *            be used by Riak HTTP client
	 * @param timeout
	 *            the connection and socket read timeout in milliseconds
	 * @param maxConnections
	 *            the maximum number of connections to the Riak REST interface
	 *            at <code>url</code> to create
	 * @param retryHandler
	 *            an implementation of {@link HttpRequestRetryHandler} to be
	 *            used by the underlying {@link HttpClient}
	 */
	private HTTPClientConfig(String url, String mapreducePath, HttpClient httpClient, Integer timeout,
							 Integer maxConnections, HttpRequestRetryHandler retryHandler) {
		try {
			this.uri = new URI(url);
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException(e);
		}

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
	 * @return an HTTP client config populated with default values;
	 * @see Builder
	 */
	public static HTTPClientConfig defaults() {
		return new Builder().build();
	}

	/**
	 * @return the URL 
	 */
	public String getUrl() {
		return uri.toString();
	}

	/**
	 * @return the mapreduce path
	 */
	public String getMapreducePath() {
		return mapreducePath;
	}

	/**
	 * @return the Apache {@link HttpClient}
	 */
	public HttpClient getHttpClient() {
		return httpClient;
	}

	/**
	 * @return the timeout in milliseconds for socket connect & read (also used
	 *         by HttpClient for pooled connection blocking acquisition timeout)
	 */
	public Integer getTimeout() {
		return timeout;
	}

	/**
	 * @return the max connections
	 */
	public Integer getMaxConnections() {
		return maxConnections;
	}

	/**
	 * @return the {@link HttpRequestRetryHandler}
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
	 * <td>/mapred</td>
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
	 * <td>null (will then use the HttpClient default which is a max of *2*)</td>
	 * </tr>
	 * <tr>
	 * <td>httpRequestRetryHandler</td>
	 * <td>null (will use the HttpClient default)</td>
	 * </tr>
	 * </table>
	 * 
	 */
	public static final class Builder {
		private URI uri = null;
		private String scheme = "http";
		private String host = "127.0.0.1";
		private int port = 8098;
		private String riakPath = "/riak";
		private String mapreducePath = "/mapred";
		private HttpClient httpClient = null;
		private Integer timeout = null;
		private Integer maxConnections = null;
		private HttpRequestRetryHandler retryHandler = null;

		/**
		 * @return a {@link HTTPClientConfig}
		 */
		public HTTPClientConfig build() {
			String builderUrl;
			if (uri == null) {
				StringBuilder sb = new StringBuilder(scheme).append("://").append(host).append(":").append(port);

				if (!riakPath.startsWith("/")) {
					sb.append("/");
				}

				builderUrl = sb.append(riakPath).toString();
			} else {
				builderUrl = uri.toString();
			}

			return new HTTPClientConfig(builderUrl, mapreducePath, httpClient, timeout, maxConnections, retryHandler);
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

			b.mapreducePath = copyConfig.mapreducePath;
			b.httpClient = copyConfig.httpClient;
			b.timeout = copyConfig.timeout;
			b.maxConnections = copyConfig.maxConnections;
			b.retryHandler = copyConfig.retryHandler;

			// This avoids the new builder from being unchangable due to
			// the withUrl() method taking precendent
			if (copyConfig.uri != null) {
				b.scheme = copyConfig.uri.getScheme();
				b.riakPath = copyConfig.uri.getRawPath();
				b.host = copyConfig.uri.getHost();
				b.port = copyConfig.uri.getPort();
			}

			return b;
		}

		/**
		 * The URL Riak REST interface.
		 * <p>
		 * NOTE: Setting this take precedence over setting <code>scheme</code>,
		 * <code>host</code>, <code>port</code> and <code>path</code>
		 * </p>
		 * 
		 * @param url
		 *            the Riak REST URL
		 * @return this
		 */
		public Builder withUrl(String url) {
			try {
				this.uri = new URI(url);
			} catch (URISyntaxException ex) {
				// no-op to not change behavior. This will be caught in build()
				// when the constructor for HTTPClientConfig is called
			}
			return this;
		}

		/**
		 * Set the scheme.
		 * <p>
		 * NOTE: setting the <code>url</code> takes precedence.
		 * </p>
		 * 
		 * @param scheme
		 *            HTTP or HTTPS
		 * @return this
		 */
		public Builder withScheme(String scheme) {
			this.scheme = scheme;
			return this;
		}

		/**
		 * Set the host.
		 * <p>
		 * NOTE: setting the <code>url</code> takes precedence.
		 * </p>
		 * 
		 * @param host
		 * @return this
		 */
		public Builder withHost(String host) {
			this.host = host;
			return this;
		}

		/**
		 * Set the port.
		 * <p>
		 * NOTE: setting the <code>url</code> takes precedence.
		 * </p>
		 * 
		 * @param port
		 * @return this
		 */
		public Builder withPort(int port) {
			this.port = port;
			return this;
		}

		/**
		 * Set the path to the base Riak REST resource.
		 * <p>
		 * NOTE: setting the <code>url</code> takes precedence.
		 * </p>
		 * 
		 * @param path
		 *            e.g. /riak
		 * @return this
		 */
		public Builder withRiakPath(String path) {
			this.riakPath = path;
			return this;
		}

		/**
		 * The location of Riak's map reduce resource
		 * 
		 * @param path
		 *            e.g. /mapreduce
		 * @return this
		 */
		public Builder withMapreducePath(String path) {
			this.mapreducePath = path;
			return this;
		}

		/**
		 * You can supply a preconfigured HttpClient that you want the
		 * {@link RawClient} to delegate to.
		 * 
		 * @param client
		 *            a implementation of {@link HttpClient}
		 * @return this
		 */
		public Builder withHttpClient(HttpClient client) {
			this.httpClient = client;
			return this;
		}

		/**
		 * The connection, socket read and pooled connection acquisition timeout
		 * in milliseconds
		 * 
		 * @param timeout
		 *            in milliseconds
		 * @return this
		 */
		public Builder withTimeout(int timeout) {
			this.timeout = timeout;
			return this;
		}

		/**
		 * @deprecated As of 1.0.8, replaced by {@link #withMaxConnections(int)}
		 * @see #withMaxConnections(int)
		 */
		@Deprecated
		public Builder withMaxConnctions(int maxConnections) {
			return withMaxConnections(maxConnections);
		}

		/**
		 * Maximum number of connections this client may have open at any time.
		 *
		 * @param maxConnections The maximum of connections open in this client.
		 * @return this
		 */
		public Builder withMaxConnections(int maxConnections) {
			this.maxConnections = maxConnections;
			return this;
		}

		/**
		 * Apache HttpClient treats some HTTP methods as retry-able, and
		 * provides a default implementation of {@link HttpRequestRetryHandler}
		 * to be called when a request fails. Provide an implementation here to
		 * override the default behaviour.
		 * 
		 * @param retryHandler
		 *            an {@link HttpRequestRetryHandler} implementation
		 * @return this
		 */
		public Builder withRetryHandler(HttpRequestRetryHandler retryHandler) {
			this.retryHandler = retryHandler;
			return this;
		}
	}
}
