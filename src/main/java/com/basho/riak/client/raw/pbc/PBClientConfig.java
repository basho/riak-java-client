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
package com.basho.riak.client.raw.pbc;

import com.basho.riak.client.raw.config.Configuration;

/**
 * The set of configuration parameters needed to configure a Protocol Buffers
 * client
 * 
 * @author russell
 * 
 */
public class PBClientConfig implements Configuration {

    private final int socketBufferSizeKb;
    private final String host;
    private final int port;
    private final int poolSize;
    private final int initialPoolSize;
    private final long idleConnectionTTLMillis;
    private final long connectionWaitTimeoutMillis;

    /**
     * Creates a new {@link PBClientConfig} instance. Use the {@link Builder}
     * 
     * @param socketBufferSizeKb
     *            the size for the protocol buffer socket's read, write and
     *            socket buffers (in kilobytes). Total buffers per connection
     *            will be 3 times this value
     * @param host
     *            the host address for the Riak protocol buffers interface
     * @param port
     *            the port for the Riak protocol buffers interface
     * @param poolSize
     *            the hard limit for the connection pool
     * @param initialPoolSize
     *            the initial size for the connection pool. The connection pool
     *            will create this many connections when the pool starts up.
     * @param idleConnectionTTLMillis
     *            how many milliseconds an idle connection survives in the pool
     *            before it is closed.
     * @param connectionWaitTimeoutMillis
     *            How many milliseconds to block trying to obtain a connection
     *            from the pool before failing
     */
    private PBClientConfig(int socketBufferSizeKb, String host, int port, int poolSize, int initialPoolSize,
            long idleConnectionTTLMillis, long connectionWaitTimeoutMillis) {
        this.socketBufferSizeKb = socketBufferSizeKb;
        this.host = host;
        this.port = port;
        this.poolSize = poolSize;
        this.initialPoolSize = initialPoolSize;
        this.idleConnectionTTLMillis = idleConnectionTTLMillis;
        this.connectionWaitTimeoutMillis = connectionWaitTimeoutMillis;
    }

    /**
     * Create an instance with all default values. See the builder for the
     * default values.
     * 
     * @return an instance configured as per the builder defaults
     * @see Builder
     */
    public static PBClientConfig defaults() {
        return new Builder().build();
    }

    /**
     * @return the size of each buffer for the connection (1 each for read,
     *         write and socket)
     */
    public int getSocketBufferSizeKb() {
        return socketBufferSizeKb;
    }

    /**
     * @return the host address of the pb interface
     */
    public String getHost() {
        return host;
    }

    /**
     * @return the port
     */
    public int getPort() {
        return port;
    }

    /**
     * @return the total maximum connection pool size
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * @return the initial pool size
     */
    public int getInitialPoolSize() {
        return initialPoolSize;
    }

    /**
     * @return the TTL (in milliseconds) of idle connections in the pool
     */
    public long getIdleConnectionTTLMillis() {
        return idleConnectionTTLMillis;
    }

    /**
     * @return the how long to block (in milliseconds) when acquiring a
     *         connection
     */
    public long getConnectionWaitTimeoutMillis() {
        return connectionWaitTimeoutMillis;
    }

    /**
     * Builder for the {@link PBClientConfig} Has the following default values:
     * 
     * <table>
     * <tr>
     * <th>field</th>
     * <th>default</th>
     * </tr>
     * <tr>
     * <td>socketBufferSizeKb</td>
     * <td>16</td>
     * </tr>
     * <tr>
     * <td>host</td>
     * <td>127.0.0.1</td>
     * </tr>
     * <tr>
     * <td>port</td>
     * <td>8097</td>
     * </tr>
     * <tr>
     * <td>poolSize</td>
     * <td>0 (unlimited)</td>
     * </tr>
     * <tr>
     * <td>initialPoolSize</td>
     * <td>0</td>
     * </tr>
     * <tr>
     * <td>idleConnectionTTLMillis</td>
     * <td>1000 (idle connections will be closed after 1 second)</td>
     * </tr>
     * <tr>
     * <td>connectionWaitTimeoutMillis</td>
     * <td>1000 (if a connection cannot be acquired within this time and
     * exception is thrown)</td>
     * </tr>
     * </table>
     * 
     * @author russell
     */
    public static final class Builder {
        private int socketBufferSizeKb = 16;
        private String host = "127.0.0.1";
        private int port = 8097;
        private int poolSize = 0;
        private int initialPoolSize = 0;
        private long idleConnectionTTLMillis = 1000;
        private long connectionWaitTimeoutMillis = 1000;

        public PBClientConfig build() {
            return new PBClientConfig(socketBufferSizeKb, host, port, poolSize, initialPoolSize,
                                      idleConnectionTTLMillis, connectionWaitTimeoutMillis);
        }

        /**
         * Create a new builder with values all copied from
         * <code>copyConfig</code>
         * 
         * @param copyConfig
         *            the {@link PBClientConfig} to copy values from
         * @return a new {@link PBClientConfig.Builder} populated with
         *         <code>copyConfig</code>'s values.
         */
        public static PBClientConfig.Builder from(PBClientConfig copyConfig) {
            Builder b = new Builder();
            b.socketBufferSizeKb = copyConfig.socketBufferSizeKb;
            b.host = copyConfig.host;
            b.port = copyConfig.port;
            b.poolSize = copyConfig.poolSize;
            b.initialPoolSize = copyConfig.initialPoolSize;
            b.idleConnectionTTLMillis = copyConfig.idleConnectionTTLMillis;
            b.connectionWaitTimeoutMillis = copyConfig.connectionWaitTimeoutMillis;
            return new PBClientConfig.Builder();
        }

        public Builder withSocketBufferSizeKb(int socketBufferSizeKb) {
            this.socketBufferSizeKb = socketBufferSizeKb;
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

        public Builder withPoolSize(int poolSize) {
            this.poolSize = poolSize;
            return this;
        }

        public Builder withInitialPoolSize(int initialPoolSize) {
            this.initialPoolSize = initialPoolSize;
            return this;
        }

        public Builder withIdleConnectionTTLMillis(long idleConnectionTTLMillis) {
            this.idleConnectionTTLMillis = idleConnectionTTLMillis;
            return this;
        }

        public Builder withConnectionTimeoutMillis(long connectionTimeoutMillis) {
            this.connectionWaitTimeoutMillis = connectionTimeoutMillis;
            return this;
        }
    }
}
