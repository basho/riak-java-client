/**
 * This file is part of riak-java-pb-client 
 *
 * Copyright (c) 2010 by Trifork
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.basho.riak.pbc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import com.basho.riak.pbc.RPB.RpbErrorResp;
import com.google.protobuf.MessageLite;

/**
 * Wraps the {@link Socket} used to send/receive data to Riak's protocol buffers interface.
 * 
 * See <a href="http://wiki.basho.com/PBC-API.html">Basho Wiki</a> for more details.
 */
class RiakConnection {

	static final int DEFAULT_RIAK_PB_PORT = 8087;

	 private Socket sock;
	 private DataOutputStream dout;
	 private DataInputStream din;
	 private final RiakConnectionPool pool;
	 // Guarded by the intrinsic lock 'this'
	 private byte[] clientId;

    private volatile long idleStart;

	public RiakConnection(InetAddress addr, int port, int bufferSizeKb, final RiakConnectionPool pool) throws IOException {
        this(new InetSocketAddress(addr, port), bufferSizeKb, pool, 0);
    }

	public RiakConnection(InetAddress addr, int port, int bufferSizeKb, final RiakConnectionPool pool, final long timeoutMillis) throws IOException {
        this(new InetSocketAddress(addr, port), bufferSizeKb, pool, timeoutMillis);
    }

	public RiakConnection(SocketAddress addr, int bufferSizeKb, final RiakConnectionPool pool, final long timeoutMillis) throws IOException {
	    if(timeoutMillis > Integer.MAX_VALUE || timeoutMillis < Integer.MIN_VALUE) {
	        throw new IllegalArgumentException("Cannot cast timeout to int without changing value");
	    }

	    this.pool = pool;
        sock = new Socket();
        sock.connect(addr, (int)timeoutMillis);

        sock.setSendBufferSize(1024 * bufferSizeKb);

        dout = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream(), 1024 * bufferSizeKb));
        din = new DataInputStream(new BufferedInputStream(sock.getInputStream(), 1024 * bufferSizeKb));
    }

	///////////////////////

	void send(int code, MessageLite req) throws IOException {
		try {
            int len = req.getSerializedSize();
            dout.writeInt(len + 1);
            dout.write(code);
            req.writeTo(dout);
            dout.flush();
        } catch (IOException e) {
            // Explicitly close our Socket on an IOException then rethrow
            close();
            throw e;
        }
	}

	void send(int code) throws IOException {
		try {
            dout.writeInt(1);
            dout.write(code);
            dout.flush();
        } catch (IOException e) {
            // Explicitly close our Socket on an IOException then rethrow
            close();
            throw e;
        }
        
	}

	byte[] receive(int code) throws IOException {
		
        int len;
        int get_code;
        byte[] data = null;
        
        
        try {
            len = din.readInt();
            get_code = din.read();

            if (len > 1) {
                data = new byte[len - 1];
                din.readFully(data);
            }
        } catch (IOException e) {
            // Explicitly close our Socket on an IOException then rethrow
            close();
            throw e;
        }
            
        if (get_code == RiakClient.MSG_ErrorResp) {
            RpbErrorResp err = com.basho.riak.pbc.RPB.RpbErrorResp.parseFrom(data);
            throw new RiakError(err);
        }

        if (code != get_code) {
            throw new IOException("bad message code. Expected: " + code + " actual: " + get_code);
        }

		return data;
        
        
	}

	void receive_code(int code) throws IOException, RiakError {
		
        int len;
        int get_code;
        
        try {
            len = din.readInt();
            get_code = din.read();
            if (code == RiakClient.MSG_ErrorResp) {
                RpbErrorResp err = com.basho.riak.pbc.RPB.RpbErrorResp.parseFrom(din);
                throw new RiakError(err);
            }
        } catch (IOException e) {
            // Explicitly close our Socket on an IOException then rethrow
            close();
            throw e;
        }
            
        if (len != 1 || code != get_code) {
            throw new IOException("bad message code");
        }
        
	}


	void close() {
		if (isClosed())
			return;
		
		try {
			sock.close();
			din = null;
			dout = null;
			sock = null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	boolean checkValid() {
	    return isClosed();
	}

	public DataOutputStream getOutputStream() {
		return dout;
	}

	public boolean isClosed() {
		return sock == null || sock.isClosed();
	}

	public synchronized void beginIdle() {
	    this.idleStart = System.nanoTime();
	}
	
	public long getIdleStartTimeNanos() {
       return this.idleStart;
    }
	
    /**
     * 
     */
    public void release() {
        pool.releaseConnection(this);
    }

    /**
     * @return the clientId
     */
    public synchronized byte[] getClientId() {
        return clientId == null? null : clientId.clone();
    }

    /**
     * @param clientId the clientId to set
     */
    public synchronized void setClientId(byte[] clientId) {
        this.clientId = clientId == null? null : clientId.clone();
    }

    /**
     * @return true if a clientId has been *explicitly set* (IE not default from
     *         Riak server) on this connection
     */
    public synchronized boolean hasClientId() {
        return clientId != null && clientId.length > 0;
    }
}
