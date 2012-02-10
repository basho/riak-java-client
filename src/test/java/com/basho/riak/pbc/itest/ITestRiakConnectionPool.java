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
package com.basho.riak.pbc.itest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import com.basho.riak.client.AllTests;
import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.http.Hosts;
import com.basho.riak.client.query.functions.JSSourceFunction;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterConfig;
import com.basho.riak.pbc.AcquireConnectionTimeoutException;
import com.basho.riak.pbc.PublicRiakConnection;
import com.basho.riak.pbc.RiakClient;
import com.basho.riak.pbc.RiakConnectionPool;

/**
 * @author russell
 * 
 */
public class ITestRiakConnectionPool {

    private static final int PORT = Hosts.RIAK_PORT;
    private static final String HOST = Hosts.RIAK_HOST;

    /**
     * Test method
     * {@link com.basho.riak.pbc.RiakConnectionPool#getConnection(byte[])}.
     */
    @Test public void getConnection() throws IOException {
        final InetAddress host = InetAddress.getByName(HOST);
        byte[] clientId = new byte[] { 13, 45, 99, 2 };
        // create a pool
        final RiakConnectionPool pool = new RiakConnectionPool(0, 1, host, PORT, 1000, 16, 5000);
        pool.start();
        // get a connection
        PublicRiakConnection wrapper = new PublicRiakConnection(pool.getConnection(clientId));
        // release it (but retain it, bwah ha ha)
        pool.releaseConnection(wrapper.getConn());
        // get a connection (it should actually be == our connection)
        assertEquals(wrapper.getConn(), pool.getConnection(clientId));

        pool.releaseConnection(wrapper.getConn());
        pool.shutdown();
    }

    /**
     * Test method
     * {@link com.basho.riak.pbc.RiakConnectionPool#getConnection(byte[])}.
     */
    @Test public void getConnection_releaseReleases() throws IOException {
        final InetAddress host = InetAddress.getByName(HOST);
        byte[] clientId = new byte[] { 13, 45, 99, 2 };
        // create a pool
        final RiakConnectionPool pool = new RiakConnectionPool(1, 1, host, PORT, 1000, 16, 5000);
        pool.start();
        // get a connection
        PublicRiakConnection wrapper = new PublicRiakConnection(pool.getConnection(clientId));

        try {
            pool.getConnection(clientId);
        } catch (IOException e) {
            assertTrue(e instanceof AcquireConnectionTimeoutException);
        }

        pool.releaseConnection(wrapper.getConn());

        PublicRiakConnection wrapper2 = new PublicRiakConnection(pool.getConnection(clientId));
        assertEquals(wrapper.getConn(), wrapper2.getConn());
        pool.releaseConnection(wrapper2.getConn());
        pool.shutdown();
    }

    /**
     * Test method
     * {@link com.basho.riak.pbc.RiakConnectionPool#getConnection(byte[])}.
     */
    @Test public void getConnection_reaped() throws Exception {
        final InetAddress host = InetAddress.getByName(HOST);
        byte[] clientId = new byte[] { 13, 45, 99, 2 };
        final int reapTime = 500;
        // create a pool
        final RiakConnectionPool pool = new RiakConnectionPool(1, 1, host, PORT, 1000, 16, reapTime);
        pool.start();
        // get a connection
        PublicRiakConnection wrapper = new PublicRiakConnection(pool.getConnection(clientId));

        pool.releaseConnection(wrapper.getConn());

        Thread.sleep(reapTime + 1000);

        assertTrue(wrapper.isConnClosed());

        PublicRiakConnection wrapper2 = new PublicRiakConnection(pool.getConnection(clientId));

        assertNotSame(wrapper2.getConn(), wrapper.getConn());
        pool.shutdown();
    }

    @Test public void conncurrentAcquire_toFewConnections() throws Exception {
        int numTasks = 5;
        int maxConnections = 4;

        doConcurrentAcquire(numTasks, maxConnections);
    }

    @Test public void conncurrentAcquire_ampleConnections() throws Exception {
        int numTasks = 5;
        int maxConnections = 10;

        doConcurrentAcquire(numTasks, maxConnections);
    }

    @Test public void getConnection_clusterpoolshutdown() throws Exception {
        PBClusterConfig config = new PBClusterConfig(4);
        config.addClient(new PBClientConfig.Builder().withHost(HOST).withPort(PORT).build());

        IRiakClient riak=RiakFactory.newClient(config);
        riak.ping();
        riak.shutdown();
        try{
          riak.ping();
          assertTrue("Use of client after shutdown should fail",false);
        } catch(IllegalStateException e){
          // Ignored
        }
    }

    private void doConcurrentAcquire(int numTasks, int maxConnections) throws Exception {
        final InetAddress host = InetAddress.getByName(HOST);
        // create a pool
        final RiakConnectionPool pool = new RiakConnectionPool(0, maxConnections, host, PORT, 1000, 16, 0);
        pool.start();

        Collection<Future<PublicRiakConnection>> results = Executors.newFixedThreadPool(numTasks).invokeAll(makeTasks(numTasks,
                                                                                                                      pool));

        int successCount = 0;
        int failureCount = 0;

        for (Future<PublicRiakConnection> res : results) {
            try {
                if (res.get() != null) {
                    successCount++;
                }
            } catch (ExecutionException e) {
                if (e.getCause() instanceof AcquireConnectionTimeoutException) {
                    failureCount++;
                } else {
                    throw e;
                }
            }
        }

        assertEquals("Wrong number of connections acquired", Math.min(numTasks, maxConnections), successCount);
        assertEquals("Wrong number of failed connection acquisitions", Math.max(0, numTasks - maxConnections),
                     failureCount);
        pool.shutdown();
    }

    /**
     * @param numTasks
     * @param pool
     * @return
     */
    private Collection<Callable<PublicRiakConnection>> makeTasks(int numTasks, final RiakConnectionPool pool) {
        Collection<Callable<PublicRiakConnection>> tasks = new ArrayList<Callable<PublicRiakConnection>>();
        final byte[] clientId = new byte[] { 12, 12, 12, 12 };
        for (int i = 0; i < numTasks; i++) {

            tasks.add(new Callable<PublicRiakConnection>() {

                public PublicRiakConnection call() throws Exception {
                    return new PublicRiakConnection(pool.getConnection(clientId));
                }
            });

        }

        return tasks;
    }

    @Test public void shutDownWorksAfterError() throws Exception {
        final InetAddress host = InetAddress.getByName(HOST);
        final RiakConnectionPool pool = new RiakConnectionPool(0, 10, host, PORT, 1000, 16, 5000);
        pool.start();

        assertEquals("RUNNING", pool.getPoolState());
        RiakClient delegate = new RiakClient(pool);
        IRiakClient c = RiakFactory.pbcClient(delegate);

        try {
            c.mapReduce("bucket").addMapPhase(new JSSourceFunction("this is not javascript")).execute();
        } catch (Exception e) {
            // NO-OP
        }

        c.shutdown();

        int start = 0;
        boolean shutdown = false;

        while (start < 10 && !shutdown) {
            start = start + 1;
            shutdown = pool.getPoolState().equals("SHUTDOWN");
            Thread.sleep(1000);
        }
        assertTrue("Expected pool to shutdown within 10 seconds", shutdown);
    }

    @Test public void shutdownWorksAfterSuccessfulMapReduce() throws Exception {
        final InetAddress host = InetAddress.getByName(HOST);
        final RiakConnectionPool pool = new RiakConnectionPool(0, 10, host, PORT, 1000, 16, 5000);
        pool.start();
        assertEquals("RUNNING", pool.getPoolState());
        RiakClient delegate = new RiakClient(pool);
        IRiakClient c = RiakFactory.pbcClient(delegate);
        Bucket b = c.fetchBucket("bucket").execute();
        b.store("key", "value").execute();

        c.mapReduce("bucket").addMapPhase(NamedErlangFunction.MAP_OBJECT_VALUE).execute();

        c.shutdown();

        int start = 0;
        boolean shutdown = false;

        while (start < 10 && !shutdown) {
            start = start + 1;
            shutdown = pool.getPoolState().equals("SHUTDOWN");
            Thread.sleep(1000);
        }

        assertTrue("Expected pool to shutdown within 10 seconds", shutdown);

        AllTests.emptyBucket("bucket", RiakFactory.pbcClient(HOST, PORT));
    }
}
