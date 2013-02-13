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
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.basho.riak.client.raw.pbc.PoolSemaphore;
import com.basho.riak.protobuf.RiakKvPB.RpbSetClientIdReq;
import com.google.protobuf.ByteString;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingDeque;

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

    /**
     * Specific behaviour for the pool methods, depending on the pools current
     * state.
     */
    enum State {
        CREATED {
            @Override void releaseConnection(RiakConnection c, RiakConnectionPool pool) {
                throw new IllegalStateException("Pool not yet started");
            }

            @Override RiakConnection getConnection(byte[] clientId, RiakConnectionPool pool) {
                throw new IllegalStateException("Pool not yet started");
            }

            @Override void start(RiakConnectionPool pool) {
                pool.doStart();
            }

            @Override void shutdown(RiakConnectionPool pool) {
                pool.doShutdown();
            }
        },
        RUNNING {
            @Override void releaseConnection(RiakConnection c, RiakConnectionPool pool) {
                pool.doRelease(c);
            }

            @Override RiakConnection getConnection(byte[] clientId, RiakConnectionPool pool) throws IOException {
                return pool.doGetConection(clientId); // use the internal get
                                                      // connection method
            }

            @Override void start(RiakConnectionPool pool) {
                throw new IllegalStateException("pool already started");
            }

            @Override void shutdown(RiakConnectionPool pool) {
                pool.doShutdown();
            }
        },
        SHUTTING_DOWN {
            @Override void releaseConnection(RiakConnection c, RiakConnectionPool pool) {
                pool.closeAndRemove(c);
            }

            @Override RiakConnection getConnection(byte[] clientId, RiakConnectionPool pool) {
                throw new IllegalStateException("pool shutting down");
            }

            @Override void start(RiakConnectionPool pool) {
                throw new IllegalStateException("pool shutting down");
            }

            @Override void shutdown(RiakConnectionPool pool) {
                throw new IllegalStateException("pool shutting down");
            }
        },
        SHUTDOWN {
            @Override void releaseConnection(RiakConnection c, RiakConnectionPool pool) {
                throw new IllegalStateException("pool shut down");
            }

            @Override RiakConnection getConnection(byte[] clientId, RiakConnectionPool pool) {
                throw new IllegalStateException("pool shut down");
            }

            @Override void start(RiakConnectionPool pool) {
                throw new IllegalStateException("pool shut down");
            }

            @Override void shutdown(RiakConnectionPool pool) {
                throw new IllegalStateException("pool shut down");
            }
        };

        abstract void releaseConnection(RiakConnection c, RiakConnectionPool pool);

        abstract RiakConnection getConnection(byte[] clientId, RiakConnectionPool pool) throws IOException;

        abstract void start(RiakConnectionPool pool);

        abstract void shutdown(RiakConnectionPool pool);
    }

    /**
     * Constant to use for <code>maxSize</code> when creating an unbounded pool
     */
    public static final int LIMITLESS = 0;
    private static final int CONNECTION_ACQUIRE_ATTEMPTS = 3;
    private final InetAddress host;
    private final int port;
    private final Semaphore permits;
    private final LinkedBlockingDeque<RiakConnection> available;
    private final ConcurrentLinkedQueue<RiakConnection> inUse;
    private final long connectionWaitTimeoutNanos;
    private final int bufferSizeKb;
    private final int initialSize;
    private final long idleConnectionTTLNanos;
    private final int requestTimeoutMillis;
    private final ScheduledExecutorService idleReaper;
    private final ScheduledExecutorService shutdownExecutor;
    // what state the pool is in (see enum State)
    private volatile State state;

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
     * @param requestTimeoutMillis 
     *            The SO_TIMEOUT flag on the socket; read/write timeout
     *            0 means forever
     * @throws IOException
     *             If the initial connection creation throws an IOException
     */
    public RiakConnectionPool(int initialSize, int maxSize, InetAddress host, int port,
            long connectionWaitTimeoutMillis, int bufferSizeKb, long idleConnectionTTLMillis,
            int requestTimeoutMillis) throws IOException {
        this(initialSize, getSemaphore(maxSize), host, port, connectionWaitTimeoutMillis, bufferSizeKb,
             idleConnectionTTLMillis, requestTimeoutMillis);

        if (initialSize > maxSize && (maxSize > 0)) {
            state = State.SHUTTING_DOWN;
            throw new IllegalArgumentException("Initial pool size is greater than maximum pools size");
        }
    }

    /**
     * Crate a new host connection pool. NOTE: before using you must call
     * start()
     * 
     * @param initialSize
     *            the number of connections to create at pool creation time
     * @param clusterSemaphore
     *            a {@link Semaphore} set with the number of permits for the
     *            pool (and maybe cluster (see {@link PoolSemaphore}))
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
     * @param requestTimeoutMillis 
     *            The SO_TIMEOUT flag on the socket; read/write timeout
     *            0 means forever
     * @throws IOException
     *             If the initial connection creation throws an IOException
     */
    public RiakConnectionPool(int initialSize, Semaphore poolSemaphore, InetAddress host, int port,
            long connectionWaitTimeoutMillis, int bufferSizeKb, long idleConnectionTTLMillis,
            int requestTimeoutMillis) throws IOException {
        this.permits = poolSemaphore;
        this.available = new LinkedBlockingDeque<RiakConnection>();
        this.inUse = new ConcurrentLinkedQueue<RiakConnection>();
        this.bufferSizeKb = bufferSizeKb;
        this.host = host;
        this.port = port;
        this.connectionWaitTimeoutNanos = TimeUnit.NANOSECONDS.convert(connectionWaitTimeoutMillis, TimeUnit.MILLISECONDS);
        this.requestTimeoutMillis = requestTimeoutMillis;
        this.initialSize = initialSize;
        this.idleConnectionTTLNanos = TimeUnit.NANOSECONDS.convert(idleConnectionTTLMillis, TimeUnit.MILLISECONDS);
        this.idleReaper = Executors.newScheduledThreadPool(1);
        this.shutdownExecutor = Executors.newScheduledThreadPool(1);
        this.state = State.CREATED;
        warmUp();
    }

    /**
     * Starts the reaper thread
     */
    public synchronized void start() {
        state.start(this);
    }

    private synchronized void doStart() {
        if (idleConnectionTTLNanos > 0) {
            idleReaper.scheduleWithFixedDelay(new Runnable() {
                public void run() {
                    // Note this will not throw a ConncurrentModificationException
                    // and if hasNext() returns true you are guaranteed that
                    // the next() will return a value (even if it has already
                    // been removed from the Deque between those calls). 
                    Iterator<RiakConnection> i = available.descendingIterator();
                    while (i.hasNext()) {
                        RiakConnection c = i.next();
                        long connIdleStartNanos = c.getIdleStartTimeNanos();
                        if (connIdleStartNanos + idleConnectionTTLNanos < System.nanoTime()) {
                            if (c.getIdleStartTimeNanos() == connIdleStartNanos) {
                                // still a small window, but better than locking
                                // the whole pool
                                boolean removed = available.remove(c);
                                if (removed) {
                                    c.close();
                                    permits.release();
                                }
                            }
                        } else {
                            // Since we are descending and this is a LIFO, 
                            // if the current connection hasn't been idle beyond 
                            // the threshold, there's no reason to descend further
                            break;
                        }
                        
                    }
                }
            }, idleConnectionTTLNanos, idleConnectionTTLNanos, TimeUnit.NANOSECONDS);
        }

        state = State.RUNNING;
    }

    /**
     * Create the correct type of semaphore for the
     * <code>maxSize</maxSize>, zero is limitless.
     * 
     * @param maxSize
     *            the number of permits to create a semaphore for
     * @return a {@link Semaphore} with <code>maxSize</code> permits, or a
     *         {@link LimitlessSemaphore} if <code>maxSize</code> is zero or less.
     */
    public static Semaphore getSemaphore(int maxSize) {
        if (maxSize <= LIMITLESS) {
            return new LimitlessSemaphore();
        }
        return new Semaphore(maxSize, true);
    }

    /**
     * If there are any initial connections to create, do it.
     * @throws IOException
     */
    private void warmUp() throws IOException {
        if (permits.tryAcquire(initialSize)) {
            for (int i = 0; i < this.initialSize; i++) {
                RiakConnection c = 
                    new RiakConnection(this.host, this.port, 
                                       this.bufferSizeKb, this, 
                                       TimeUnit.MILLISECONDS.convert(connectionWaitTimeoutNanos, TimeUnit.NANOSECONDS), 
                                       requestTimeoutMillis);
                c.beginIdle();
                available.add(c);
            }
        } else {
            throw new RuntimeException("Unable to create initial connections");
        }
    }

    /**
     * Get a connection from the pool for the given client Id. If there is a
     * connection with that client id in the pool, get that, if there is a
     * connection in the pool but with the wrong Id, call setClientId on it and
     * return it, if there is no available connection and the pool is under
     * limit create a connection, set the id on it and return it.
     * 
     * @param clientId
     *            the client id of the connection requested
     * @return a RiakConnection with the clientId set
     * @throws IOException
     * @throws AcquireConnectionTimeoutException
     *             if unable to acquire a permit to create a *new* connection
     *             within the timeout configured. This means that the pool has
     *             no available connections and there are no permits available to
     *             create new connections. Repeated incidences of this exception
     *             probably indicate that you have sized your pool too small.
     */
    public RiakConnection getConnection(byte[] clientId) throws IOException {
       return state.getConnection(clientId, this);
    }

    private RiakConnection doGetConection(byte[] clientId) throws IOException {
        RiakConnection c = getConnection();
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
        RpbSetClientIdReq req = com.basho.riak.protobuf.RiakKvPB.RpbSetClientIdReq.newBuilder().setClientId(ByteString.copyFrom(clientId)).build();

        try {
            c.send(RiakMessageCodes.MSG_SetClientIdReq, req);
            c.receive_code(RiakMessageCodes.MSG_SetClientIdResp);
            c.setClientId(clientId);
        } catch (IOException e) {
            // can't set the clientId? Can't use the connection. Kill it and
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
    private RiakConnection getConnection() throws IOException {
        RiakConnection c = available.poll();

        if (c == null) {
           c = createConnection(CONNECTION_ACQUIRE_ATTEMPTS);
        }

        inUse.offer(c);
        return c;
    }

    /**
     * @param attempts
     * @return
     */
    private RiakConnection createConnection(int attempts) throws IOException {
        try {
            if (permits.tryAcquire(connectionWaitTimeoutNanos, TimeUnit.NANOSECONDS)) {
                boolean releasePermit = true;
                try {
                    RiakConnection connection = new RiakConnection(host, port, bufferSizeKb, this, TimeUnit.MILLISECONDS.convert(connectionWaitTimeoutNanos, TimeUnit.NANOSECONDS), requestTimeoutMillis);
                    releasePermit = false;
                    return connection;
                } catch (SocketTimeoutException e) {
                    throw new AcquireConnectionTimeoutException("timeout from socket connection " + e.getMessage(), e);
                } catch (IOException e) {
                    throw e;
                } finally {
                    if (releasePermit) {
                        permits.release();
                    }
                }
            } else {
                throw new AcquireConnectionTimeoutException("timeout acquiring connection permit from pool");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            
            if (attempts > 0) {
                return createConnection(attempts - 1);
            } else {
                throw new IOException("repeatedly interrupted whilst waiting to acquire connection");
            }
        }
    }

    /**
     * Returns a connection to the pool (unless the connection is closed (for some
     * reason))
     * 
     * @param c
     *            the connection to return.
     */
    public void releaseConnection(final RiakConnection c) {
        state.releaseConnection(c, this);
    }

    /**
     * internal release
     * @param c
     */
    private void doRelease(RiakConnection c) {
        if (c == null) {
            return;
        }

        if (inUse.remove(c)) {
            if (!c.isClosed()) {
                c.beginIdle();
                available.offerFirst(c);
            } else {
                // don't put a closed connection in the pool, release a permit
                permits.release();
            }
        } else {
            // not our connection?
            throw new IllegalArgumentException("connection not managed by this pool");
        }
    }

    /**
     * internal close and remove from the inUse queue. Does not return to the
     * available pool, does not release a permit. Used for a pool that is
     * shutting/shut down.
     * 
     * @param c
     */
    private void closeAndRemove(RiakConnection c) {
        c.close();
        inUse.remove(c);
    }

    /**
     * Close this pool and all its connections. The pool will throw
     * {@link IllegalStateException} for calls to getConnection. While shutting
     * down it will still accept calls to releaseConnection.
     */
    public synchronized void shutdown() {
        state.shutdown(this);
    }

    /**
     * Shuts the pool down.
     * 
     * Close all connections in available.
     * Stop the reaper thread.
     * Start a thread that checks the inUse queue is empty.
     */
    private void doShutdown() {
        state = State.SHUTTING_DOWN;

        // drain the available pool
        RiakConnection c = available.poll();
        while( c != null) {
            c.close();
            c = available.poll();
        }

        shutdownExecutor.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                // when all connections are returned, and the available pool is empty
                if(inUse.isEmpty() && available.isEmpty()) {
                    state = State.SHUTDOWN;
                    shutdownExecutor.shutdown();
                    idleReaper.shutdown();
                }
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    /**
     * Convenience method to check the state of the pool.
     * 
     * @return the name of the current {@link State} of the pool
     */
    public String getPoolState() {
        return state.name();
    }
}
