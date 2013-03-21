package com.basho.riak.pbc;

import java.io.IOException;
import java.net.InetAddress;

public class RiakConnectionFactory {

    private final InetAddress host;
    private final int port;
    private final int bufferSizeKb;
    private final long connectionWaitTimeoutMillis;
    private final int requestTimeoutMillis;

    public RiakConnectionFactory(
            final InetAddress host,
            final int port,
            final int bufferSizeKb,
            final long connectionWaitTimeoutMillis,
            final int requestTimeoutMillis
    ) {
        this.host = host;
        this.port = port;
        this.bufferSizeKb = bufferSizeKb;
        this.connectionWaitTimeoutMillis = connectionWaitTimeoutMillis;
        this.requestTimeoutMillis = requestTimeoutMillis;
    }

    public RiakConnection createConnection(final RiakConnectionPool pool) throws IOException {
        return new RiakConnection(host, port, bufferSizeKb, pool, connectionWaitTimeoutMillis, requestTimeoutMillis);
    }

    public InetAddress getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getBufferSizeKb() {
        return bufferSizeKb;
    }

    public long getConnectionWaitTimeoutMillis() {
        return connectionWaitTimeoutMillis;
    }

    public int getRequestTimeoutMillis() {
        return requestTimeoutMillis;
    }
}