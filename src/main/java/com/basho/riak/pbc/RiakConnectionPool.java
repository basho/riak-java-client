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
 * @author russell
 * 
 */
public class RiakConnectionPool {

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
    private volatile boolean started = false;

    /**
     * @param initialSize
     * @param maxSize
     * @param permits
     * @param available
     * @param isUse
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

    public synchronized void start() {
        if (!started) {
            idleReaper.scheduleWithFixedDelay(new Runnable() {
                public void run() {
                    RiakConnection c = available.peek();
                    while (c != null) {
                        if (c.getIdleStartTimeMillis() + idleConnectionTTLMillis < System.currentTimeMillis()) {
                            // is this safe? maybe the connection was used and returned to the queue in the time the above executed?
                            boolean removed = available.remove(c);
                            if (removed) {
                                System.out.println("reaped a connection");
                                permits.release();
                            }
                            c = available.peek();
                        } else {
                            c = null;
                        }
                    }
                }
            }, connectionWaitTimeoutMillis, connectionWaitTimeoutMillis, TimeUnit.MILLISECONDS);
        }
        
        started = true;
    }

    /**
     * @param maxSize
     * @return
     */
    private Semaphore getSemaphore(int maxSize) {
        if (maxSize < 0) {
            return new LimitlessSemaphore(0);
        }
        return new Semaphore(maxSize, true);
    }

    private void warmUp() throws IOException {
        for (int i = 0; i < this.initialSize; i++) {
            available.add(new RiakConnection(this.host, this.port, this.bufferSizeKb, this));
        }
    }

    public RiakConnection getConnection(byte[] clientId) throws IOException {
        RiakConnection c = getConnection();
        if (clientId != null && !Arrays.equals(clientId, c.getClientId())) {
            setClientIdOnConnection(c, clientId);
        }
        return c;
    }

    /**
     * @param c
     * @throws IOException
     */
    private void setClientIdOnConnection(RiakConnection c, byte[] clientId) throws IOException {
        RpbSetClientIdReq req = RPB.RpbSetClientIdReq.newBuilder().setClientId(ByteString.copyFrom(clientId)).build();

        try {
            System.out.println("setting clientId");
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

    public RiakConnection getConnection() throws IOException {
        try {
            if (permits.tryAcquire(connectionWaitTimeoutMillis, TimeUnit.MILLISECONDS)) {

                RiakConnection c = available.poll();

                if (c == null) {
                    c = new RiakConnection(host, port, bufferSizeKb, this);
                    System.out.println("made a new connection");
                }

                inUse.offer(c);
                return c;
            } else {
                throw new IOException("timeout aquiring connection permit from pool");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // maybe we should stop trying after a while? TODO shutdown gracefully
            return getConnection();
        }
    }

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
