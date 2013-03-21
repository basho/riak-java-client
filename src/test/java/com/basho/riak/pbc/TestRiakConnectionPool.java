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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import org.junit.Assert;
import org.junit.Test;

import com.basho.riak.client.http.Hosts;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author geardley
 */
public class TestRiakConnectionPool {

    public static final String BAD_HOST = "192.0.2.1"; // in the TEST-NET-1 address space. This is not expected to be in use.
    private static final int PORT = Hosts.RIAK_PORT;
    private static final byte[] CLIENT_ID = new byte[] { 13, 45, 99, 2 };
    
    /**
     * Test method {@link com.basho.riak.pbc.RiakConnectionPool#getConnection(byte[])}.
     * Ensures that permits are not leak in event of timeout
     */
    @Test public void getConnection_socketConnectionTimeout() throws IOException {
        final InetAddress host = InetAddress.getByName(BAD_HOST);
        int maxConnections = 1;
        // create a pool
        final RiakConnectionPool pool = new RiakConnectionPool(0, maxConnections, host, PORT, 1, 16, 5000, 0);
        pool.start();
        
        for (int i = 0; i < maxConnections+10; i++) {
            try {
                pool.getConnection(CLIENT_ID);
                Assert.fail("Should not have been able to acquire a connection to riak!");
            } catch (AcquireConnectionTimeoutException e) {
                Assert.assertTrue("expected timeout connecting to host", e.getCause() instanceof SocketTimeoutException);
            }
        }

        assertPoolState(pool, maxConnections, 0);
        
    }

    /**
     * Make sure connection pool calls do not block and timeout attempting to get a new connection when a connection has been returned to the pool
     */
    @Test public void getConnection_noConnectionTimeoutWhenConnectionReturnedToPool() throws Exception {

        RiakConnection mockRiakConnection = mock(RiakConnection.class);

        RiakConnectionFactory mockRiakConnectionFactory = mock(RiakConnectionFactory.class);
        when(mockRiakConnectionFactory.createConnection(any(RiakConnectionPool.class))).thenReturn(mockRiakConnection);

        Semaphore semaphore = new Semaphore(1);
        final RiakConnectionPool pool = new RiakConnectionPool(1, semaphore, 5000, 5000, mockRiakConnectionFactory);
        pool.start();

        // get the first connection
        RiakConnection connection = pool.getConnection(CLIENT_ID);
        assertNotNull(connection);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<RiakConnection> future = executor.submit(new Callable<RiakConnection>() {
                public RiakConnection call() throws Exception {
                // attempt to get the second connection, should block
                return pool.getConnection(CLIENT_ID);
                }
            });
            // wait until the thread is blocked on the semaphore
            while (!semaphore.hasQueuedThreads()) { Thread.sleep(10); }
            // release connection and validate we do not time out, but get a valid connection instead
            pool.releaseConnection(connection);
            assertEquals("same connection should be returned by pool", future.get(), connection);
            verify(mockRiakConnectionFactory).createConnection(any(RiakConnectionPool.class));
        } catch(ExecutionException ee) {
            if (ee.getCause() instanceof AcquireConnectionTimeoutException) {
                fail("getConnection call should not have timed out, a connection was returned to the pool");
            } else {
                throw ee;
            }
        } finally {
            pool.shutdown();
            executor.shutdownNow();
        }
    }

    @Test public void poolConnectionCountChecks() throws Exception {

        RiakConnection mockRiakConnection = mock(RiakConnection.class);

        RiakConnectionFactory mockRiakConnectionFactory = mock(RiakConnectionFactory.class);
        when(mockRiakConnectionFactory.createConnection(any(RiakConnectionPool.class))).thenReturn(mockRiakConnection);

        Semaphore semaphore = new Semaphore(2);
        RiakConnectionPool pool = new RiakConnectionPool(1, semaphore, 100, 5000, mockRiakConnectionFactory);
        pool.start();

        try {
            assertPoolState(pool, 2, 0);

            RiakConnection connection = pool.getConnection(CLIENT_ID);
            assertPoolState(pool, 1, 1);

            pool.releaseConnection(connection);
            assertPoolState(pool, 2, 0);

            // wait for idle reaper
            Thread.sleep(200);
            assertPoolState(pool, 2, 0);

            pool.getConnection(CLIENT_ID);
            assertPoolState(pool, 1, 1);

            verify(mockRiakConnectionFactory, times(2)).createConnection(any(RiakConnectionPool.class));
        } finally {
            pool.shutdown();
        }

    }

    private void assertPoolState(RiakConnectionPool pool, int expectedNotInUseCount, int expectedInUseCount) {
        assertEquals(expectedNotInUseCount, pool.getNotInUseCount());
        assertEquals(expectedInUseCount, pool.getInUseCount());
    }
    
}
