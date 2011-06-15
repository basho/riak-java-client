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
import java.net.Socket;
import java.util.Timer;
import java.util.TimerTask;

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

	public RiakConnection(InetAddress addr, int port, int bufferSizeKb) throws IOException {
	    sock = new Socket(addr, port);

	    sock.setSendBufferSize(1024 * bufferSizeKb);

	    dout = new DataOutputStream(new BufferedOutputStream(sock
                .getOutputStream(), 1024 * bufferSizeKb));
        din = new DataInputStream(
                new BufferedInputStream(sock.getInputStream(), 1024 * bufferSizeKb));
    }

	///////////////////////

	void send(int code, MessageLite req) throws IOException {
		int len = req.getSerializedSize();
		dout.writeInt(len + 1);
		dout.write(code);
		req.writeTo(dout);
		dout.flush();
	}

	void send(int code) throws IOException {
		dout.writeInt(1);
		dout.write(code);
		dout.flush();
	}

	byte[] receive(int code) throws IOException {
		int len = din.readInt();
		int get_code = din.read();

		byte[] data = null;
        if (len > 1) {
            data = new byte[len - 1];
            din.readFully(data);
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
		int len = din.readInt();
		int get_code = din.read();
		if (code == RiakClient.MSG_ErrorResp) {
			RpbErrorResp err = com.basho.riak.pbc.RPB.RpbErrorResp.parseFrom(din);
			throw new RiakError(err);
		}
		if (len != 1 || code != get_code) {
			throw new IOException("bad message code");
		}
	}

	static Timer TIMER = new Timer("riak-client-timeout-thread", true);
	TimerTask idle_timeout;
	
	public void beginIdle() {
		idle_timeout = new TimerTask() {
			
			@Override
			public void run() {
				RiakConnection.this.timer_fired(this);
			}
		};
		
		TIMER.schedule(idle_timeout, 1000);
	}

	synchronized void timer_fired(TimerTask fired_timer) {
		if (idle_timeout != fired_timer) {
			// if it is not our current timer, then ignore
			return;
		}
		
		close();
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

	synchronized boolean endIdleAndCheckValid() {
		TimerTask tt = idle_timeout;
		if (tt != null) { tt.cancel(); }
		idle_timeout = null;
		
		if (isClosed()) {
			return false;
		} else {
			return true;
		}
	}

	public DataOutputStream getOutputStream() {
		return dout;
	}

	public boolean isClosed() {
		return sock == null || sock.isClosed();
	}
	
	
}
