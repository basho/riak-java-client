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

import org.junit.Assert;
import org.junit.Test;

import com.basho.riak.client.http.Hosts;

/**
 * @author geardley
 */
public class TestRiakConnectionPool {
    
    public static final String BAD_HOST = "192.0.2.1"; // in the TEST-NET-1 address space. This is not expected to be in use.
    private static final int PORT = Hosts.RIAK_PORT;
    
    /**
     * Test method {@link com.basho.riak.pbc.RiakConnectionPool#getConnection(byte[])}.
     * Ensures that permits are not leak in event of timeout
     */
    @Test public void getConnection_socketConnectionTimeout() throws IOException {
        final InetAddress host = InetAddress.getByName(BAD_HOST);
        byte[] clientId = new byte[] { 13, 45, 99, 2 };
        int maxConnections = 1;
        // create a pool
        final RiakConnectionPool pool = new RiakConnectionPool(0, maxConnections, host, PORT, 1, 16, 5000, 0);
        pool.start();
        
        for (int i = 0; i < maxConnections+10; i++) {
            try {
                pool.getConnection(clientId);
                Assert.fail("Should not have been able to acqurie a connection to riak!");
            } catch (AcquireConnectionTimeoutException e) {
                Assert.assertTrue("expected timeout connecting to host", e.getCause() instanceof SocketTimeoutException);
            }
        }
        
    }
    
}
