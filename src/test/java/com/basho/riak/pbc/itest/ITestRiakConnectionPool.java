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

import org.junit.Test;

import com.basho.riak.pbc.AcquireConnectionTimeoutException;
import com.basho.riak.pbc.PublicRiakConnection;
import com.basho.riak.pbc.RiakConnectionPool;

/**
 * @author russell
 * 
 */
public class ITestRiakConnectionPool {

    private static final int PORT = 8087;
    private static final String HOST = "127.0.0.1";

    /**
     * Test method
     * {@link com.basho.riak.pbc.RiakConnectionPool#getConnection(byte[])}.
     */
    @Test public void getConnection() throws IOException {
        final InetAddress host = InetAddress.getByName(HOST);
        byte[] clientId = new byte[] { 13, 45, 99, 2 };
        // create a pool
        final RiakConnectionPool pool = new RiakConnectionPool(0, 1, host, PORT, 1000, 16, 5000);
        // get a connection
        PublicRiakConnection wrapper = new PublicRiakConnection(pool.getConnection(clientId));
        // release it (but retain it, bwah ha ha)
        pool.releaseConnection(wrapper.getConn());
        // get a connection (it should actually be == our connection)
        assertEquals(wrapper.getConn(), pool.getConnection(clientId));

        pool.releaseConnection(wrapper.getConn());
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
    }
}
