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

/**
 * Wraps a RiakConnection so there is no need for the ITest to import a package
 * private class
 * 
 * @author russell
 * 
 */
public class PublicRiakConnection {
    private final RiakConnection conn;

    /**
     * @param conn
     */
    public PublicRiakConnection(RiakConnection conn) {
        this.conn = conn;
    }

    /**
     * @return the conn
     */
    public synchronized RiakConnection getConn() {
        return conn;
    }

    /**
     * @return
     */
    public boolean isConnClosed() {
        return conn.isClosed();
    }

}
