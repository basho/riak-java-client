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
package com.basho.riak.pbc;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.basho.riak.pbc.RPB.RpbSetClientIdReq;
import com.google.protobuf.ByteString;

/**
 * A bounded or boundless pool of {@link RiakConnection}s to be reused by {@link RiakClient}
 * 
 * The pool is designed to be threadsafe, and ideally to be used as a singleton.
 * Due to backwards compatibility requirements it has not been implemented as a singleton.
 * This is really a host connection pool. There is a minor optimization for reusing a connection
 * by client Id, but more work needs doing here.
 * 
 * @author russell
 * 
 */
public class RiakConnectionPool {

    private static final int CONNECTION_ACQUIRE_ATTEMPS = 3;
    private final InetAddress host;
    private final int port;
    private final Semaphore permits;
    private final ConcurrentLinkedQueue<RiakConnection> available;
    private final ConcurrentLinkedQueue<RiakConnection> inUse;
    private final long connectionWaitTimeoutMillis;
    private final int bufferSizeKb;
    private final int initialSize;
    private final long idleConnectionTTLMillis;
    private final ScheduledExecutorService idleReaper;
    //guarded by intrinsic lock, set once only on start
    private boolean started = false;

    /**
     * Crate a new host connection pool. NOTE: before using you must call
     * start()
     * 
     * @param initialSize
     *            the number of connections to create at pool creation time
     * @param maxSize
     *            the maximum number of connections this pool will have at any
     *            one time, 0 means limitless (i.e. creates a new connection if
     *            none are available)
     * @param host
     *            the host this pool holds connections to
     * @param port
     *            the port on host that this pool holds connections to
     * @param connectionWaitTimeoutMillis
     *            the connection timeout
     * @param bufferSizeKb
     *            the size of the socket/stream read/write buffers (3 buffers,
     *            each of this size)
     * @param idleConnectionTTLMillis
     *            How long for an idle connection to exist before it is reaped,
     *            0 mean forever
     * @throws IOException
     *             If the initial connection creation throws an IOException
     */
    public RiakConnectionPool(int initialSize, int maxSize, InetAddress host, int port,
            long connectionWaitTimeoutMillis, int bufferSizeKb, long idleConnectionTTLMillis) throws IOException {
        this.permits = getSemaphore(maxSize);
        this.available = new ConcurrentLinkedQueue<RiakConnection>();
        this.inUse = new ConcurrentLinkedQueue<RiakConnection>();
        this.bufferSizeKb = bufferSizeKb;
        this.host = host;
        this.port = port;
        this.connectionWaitTimeoutMillis = connectionWaitTimeoutMillis;
        this.initialSize = initialSize;
        this.idleConnectionTTLMillis = idleConnectionTTLMillis;
        this.idleReaper = Executors.newScheduledThreadPool(1);
        warmUp();
    }

    /**
     * Starts the reaper thread
     */
    public synchronized void start() {
        if (!started && idleConnectionTTLMillis > 0) {
            idleReaper.scheduleWithFixedDelay(new Runnable() {
                public void run() {
                    RiakConnection c = available.peek();
                    while (c != null) {
                        if (c.getIdleStartTimeMillis() + idleConnectionTTLMillis < System.currentTimeMillis()) {
                            // is this safe? maybe the connection was used and returned to the queue
                            // in the time the above executed?
                            boolean removed = available.remove(c);
                            if (removed) {
                                c.close();
                                permits.release();
                            }
                            c = available.peek();
                        } else {
                            c = null;
                        }
                    }
                }
            }, idleConnectionTTLMillis, idleConnectionTTLMillis, TimeUnit.MILLISECONDS);
        }

        started = true;
    }

    /**
     * Create the correct type of semaphore for the
     * <code>maxSize</maxSize>, zero is limitless.
     * 
     * @param maxSize
     *            the number of permits to create a semaphore for
     * @return a {@link Semaphore} with <code>maxSize</code> permits, or a
     *         {@link LimitlessSemaphore} is <code>maxSize</code> is zero.
     */
    private Semaphore getSemaphore(int maxSize) {
        if (maxSize < 0) {
            return new LimitlessSemaphore();
        }
        return new Semaphore(maxSize, true);
    }

    /**
     * If there are any initial connections to create, do it.
     * @throws IOException
     */
    private void warmUp() throws IOException {
        for (int i = 0; i < this.initialSize; i++) {
            available.add(new RiakConnection(this.host, this.port, this.bufferSizeKb, this));
        }
    }

    /**
     * Get a connection from the pool for the given client Id. If there is a
     * connection with that client id in the pool, get that, if there is a
     * connection in the pool but with the wrong Id, call setClientId on it and
     * return it, if there is no available connection and the pool is under
     * limit create a connection, set the id on it and return it.
     * 
     * @param clientId the client id of the connection requested
     * @return a RiakConnection with the clientId set
     * @throws IOException
     */
    public RiakConnection getConnection(byte[] clientId) throws IOException {
        RiakConnection c = getConnection(CONNECTION_ACQUIRE_ATTEMPS);
        if (clientId != null && !Arrays.equals(clientId, c.getClientId())) {
            setClientIdOnConnection(c, clientId);
        }
        return c;
    }

    /**
     * Calls the Riak PB API to set the client Id on a the given connection
     * 
     * If an exception is thrown the connection is released from the pool and
     * the connection re thrown to the caller.
     * 
     * @param c
     *            the connection to set the Id on
     * @throws IOException
     */
    private void setClientIdOnConnection(RiakConnection c, byte[] clientId) throws IOException {
        RpbSetClientIdReq req = RPB.RpbSetClientIdReq.newBuilder().setClientId(ByteString.copyFrom(clientId)).build();

        try {
            c.send(RiakMessageCodes.MSG_SetClientIdReq, req);
            c.receive_code(RiakMessageCodes.MSG_SetClientIdResp);
            c.setClientId(clientId);
        } catch (IOException e) {
            // can't set the connection? Can't use the connection. Kill it and
            // throw an IOException
            c.close();
            releaseConnection(c);
            throw e;
        }

    }

    /**
     * Get a RiakConnection from the pool, or create a new one if non in the
     * pool and the limit is not reached. Waits for the configured
     * <code>connectionWaitTimeoutMillis</code> to acquire a connection if non
     * are available, throws IOException if timeout occurs. Will re-try if
     * interrupted waiting for the connection.
     * 
     * @return a connection from the pool, or a new connection
     * @throws IOException
     */
    private RiakConnection getConnection(int attempts) throws IOException {
        try {
            if (permits.tryAcquire(connectionWaitTimeoutMillis, TimeUnit.MILLISECONDS)) {

                RiakConnection c = available.poll();

                if (c == null) {
                    try {
                        c = new RiakConnection(host, port, bufferSizeKb, this);
                    } catch (IOException e) {
                       permits.release();
                       throw e;
                    }
                }

                inUse.offer(c);
                return c;
            } else {
                throw new AcquireConnectionTimeoutException("timeout acquiring connection permit from pool");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            if (attempts > 0) {
                return getConnection(attempts - 1);
            } else {
                throw new IOException("interrupted whilst waiting to acquire connection");
            }
        }
    }

    /**
     * Returns a connection to the pool.
     * @param c the connection to return.
     */
    public void releaseConnection(final RiakConnection c) {
        if (c == null) {
            return;
        }

        if (inUse.remove(c)) {
            if (!c.isClosed()) {
                c.beginIdle();
                available.offer(c);
            }
            permits.release();
        }
    }
}
