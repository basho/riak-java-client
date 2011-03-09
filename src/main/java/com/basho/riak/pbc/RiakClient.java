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

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

import org.json.JSONObject;

import com.basho.riak.pbc.RPB.RpbDelReq;
import com.basho.riak.pbc.RPB.RpbGetClientIdResp;
import com.basho.riak.pbc.RPB.RpbGetReq;
import com.basho.riak.pbc.RPB.RpbGetResp;
import com.basho.riak.pbc.RPB.RpbGetServerInfoResp;
import com.basho.riak.pbc.RPB.RpbListBucketsResp;
import com.basho.riak.pbc.RPB.RpbMapRedReq;
import com.basho.riak.pbc.RPB.RpbPutReq;
import com.basho.riak.pbc.RPB.RpbPutResp;
import com.basho.riak.pbc.RPB.RpbSetClientIdReq;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
//import com.trifork.riak.RPB.RpbListKeysResp;
//import com.trifork.riak.RPB.RpbMapRedResp;

public class RiakClient implements RiakMessageCodes {

	private static final RiakObject[] NO_RIAK_OBJECTS = new RiakObject[0];
	private static final ByteString[] NO_BYTE_STRINGS = new ByteString[0];
	//private static final String[] NO_STRINGS = new String[0];
	//private static final MapReduceResponse[] NO_MAP_REDUCE_RESPONSES = new MapReduceResponse[0];

	private String node;
	private String serverVersion;
	private InetAddress addr;
	private int port;

	/**
	 * if this has been set (or gotten) then it will be applied to new
	 * connections
	 */
	private volatile ByteString clientID;

	public RiakClient(String host) throws IOException {
		this(host, RiakConnection.DEFAULT_RIAK_PB_PORT);
	}

	public RiakClient(String host, int port) throws IOException {
		this(InetAddress.getByName(host), port);
	}

	public RiakClient(InetAddress addr, int port) throws IOException {
		this.addr = addr;
		this.port = port;
	}

	private ThreadLocal<RiakConnection> connections = new ThreadLocal<RiakConnection>();

	RiakConnection getConnection() throws IOException {
		return getConnection(true);
	}

	RiakConnection getConnection(boolean setClientId) throws IOException {
        RiakConnection c = connections.get();
        if (c == null || !c.endIdleAndCheckValid()) {
            c = new RiakConnection(addr, port);

            if (this.clientID != null && setClientId) {
                setClientID(clientID);
            }
        }
        connections.set(null);
        return c;
    }

	void release(RiakConnection c) {
		RiakConnection cc = connections.get();
		if (cc == null) {
			c.beginIdle();
			connections.set(c);
		} else {
			c.close();
		}
	}

	/**
	 * helper method to use a reasonable default client id
	 * 
	 * @throws IOException
	 */
	public void prepareClientID() throws IOException {
		Preferences prefs = Preferences.userNodeForPackage(RiakClient.class);

		String clid = prefs.get("client_id", null);
		if (clid == null) {
			SecureRandom sr;
			try {
				sr = SecureRandom.getInstance("SHA1PRNG");
			} catch (NoSuchAlgorithmException e) {
				throw new RuntimeException(e);
			}
			byte[] data = new byte[6];
			sr.nextBytes(data);
			clid = Base64Coder.encodeLines(data);
			prefs.put("client_id", clid);
			try {
				prefs.flush();
			} catch (BackingStoreException e) {
				throw new IOException(e.toString());
			}
		}

		setClientID(clid);
	}

	public void ping() throws IOException {
		RiakConnection c = getConnection();
		try {
			c.send(MSG_PingReq);
			c.receive_code(MSG_PingResp);
		} finally {
			release(c);
		}
	}

	public void setClientID(String id) throws IOException {
		setClientID(ByteString.copyFromUtf8(id));
	}

	// /////////////////////

	public void setClientID(ByteString id) throws IOException {
		RpbSetClientIdReq req = RPB.RpbSetClientIdReq.newBuilder().setClientId(
				id).build();
		RiakConnection c = getConnection(false);
		try {
			c.send(MSG_SetClientIdReq, req);
			c.receive_code(MSG_SetClientIdResp);
		} finally {
			release(c);
		}

		this.clientID = id;
	}

	public String getClientID() throws IOException {
		RiakConnection c = getConnection();
		try {
			c.send(MSG_GetClientIdReq);
			byte[] data = c.receive(MSG_GetClientIdResp);
			if (data == null)
				return null;
			RpbGetClientIdResp res = RPB.RpbGetClientIdResp.parseFrom(data);
			clientID = res.getClientId();
			return clientID.toStringUtf8();
		} finally {
			release(c);
		}
	}

	public Map<String, String> getServerInfo() throws IOException {
		RiakConnection c = getConnection();
		try {
			c.send(MSG_GetServerInfoReq);
			byte[] data = c.receive(MSG_GetServerInfoResp);
			if (data == null)
				return Collections.emptyMap();

			RpbGetServerInfoResp res = RPB.RpbGetServerInfoResp.parseFrom(data);
			if (res.hasNode()) {
				this.node = res.getNode().toStringUtf8();
			}
			if (res.hasServerVersion()) {
				this.serverVersion = res.getServerVersion().toStringUtf8();
			}
			Map<String, String> result = new HashMap<String, String>();
			result.put("node", node);
			result.put("server_version", serverVersion);
			return result;
		} finally {
			release(c);
		}
	}

	// /////////////////////

	public RiakObject[] fetch(String bucket, String key, int readQuorum)
			throws IOException {
		return fetch(ByteString.copyFromUtf8(bucket), ByteString
				.copyFromUtf8(key), readQuorum);
	}

	public RiakObject[] fetch(ByteString bucket, ByteString key, int readQuorum)
			throws IOException {
		RpbGetReq req = RPB.RpbGetReq.newBuilder().setBucket(bucket)
				.setKey(key).setR(readQuorum).build();

		RiakConnection c = getConnection();
		try {
			c.send(MSG_GetReq, req);
			return process_fetch_reply(c, bucket, key);
		} finally {
			release(c);
		}

	}

	public RiakObject[] fetch(String bucket, String key) throws IOException {
		return fetch(ByteString.copyFromUtf8(bucket), ByteString
				.copyFromUtf8(key));
	}

	public RiakObject[] fetch(ByteString bucket, ByteString key)
			throws IOException {
		RpbGetReq req = RPB.RpbGetReq.newBuilder().setBucket(bucket)
				.setKey(key).build();

		RiakConnection c = getConnection();
		try {
			c.send(MSG_GetReq, req);
			return process_fetch_reply(c, bucket, key);
		} finally {
			release(c);
		}
	}

	private RiakObject[] process_fetch_reply(RiakConnection c,
			ByteString bucket, ByteString key) throws IOException,
			InvalidProtocolBufferException {
		byte[] rep = c.receive(MSG_GetResp);

		if (rep == null) {
			return NO_RIAK_OBJECTS;
		}

		RpbGetResp resp = RPB.RpbGetResp.parseFrom(rep);
		int count = resp.getContentCount();
		RiakObject[] out = new RiakObject[count];
		ByteString vclock = resp.getVclock();
		for (int i = 0; i < count; i++) {
			out[i] = new RiakObject(vclock, bucket, key, resp.getContent(i));
		}
		return out;
	}

	// /////////////////////

	public ByteString[] store(RiakObject[] values, RequestMeta meta)
			throws IOException {

		RiakConnection c = getConnection();
		try {
			BulkReader reader = new BulkReader(c, values.length);
			Thread worker = new Thread(reader);
			worker.start();

			DataOutputStream dout = c.getOutputStream();

			for (int i = 0; i < values.length; i++) {
				RiakObject value = values[i];

				RPB.RpbPutReq.Builder builder = RPB.RpbPutReq.newBuilder()
						.setBucket(value.getBucketBS())
						.setKey(value.getKeyBS()).setContent(
								value.buildContent());

				if (value.getVclock() != null) {
					builder.setVclock(value.getVclock());
				}

				builder.setReturnBody(false);

				if (meta != null) {

					if (meta.writeQuorum != null) {
						builder.setW(meta.writeQuorum.intValue());
					}

					if (meta.durableWriteQuorum != null) {
						builder.setDw(meta.durableWriteQuorum.intValue());
					}
				}

				RpbPutReq req = builder.build();

				int len = req.getSerializedSize();
				dout.writeInt(len + 1);
				dout.write(MSG_PutReq);
				req.writeTo(dout);
			}

			dout.flush();

			try {
				worker.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			return reader.vclocks;
		} finally {
			release(c);
		}
	}

	class BulkReader implements Runnable {

		private ByteString[] vclocks;
		private final RiakConnection c;

		public BulkReader(RiakConnection c, int count) {
			this.c = c;
			this.vclocks = new ByteString[count];
		}

		public void run() {

			try {
				for (int i = 0; i < vclocks.length; i++) {
					byte[] data = c.receive(MSG_PutResp);
					if (data != null) {
						RpbPutResp resp = RPB.RpbPutResp.parseFrom(data);
						if (resp.hasVclock()) {
							vclocks[i] = resp.getVclock();
						}
					}
				}
			} catch (IOException e) {
				// TODO
				e.printStackTrace();
			}

		}

	}

	public void store(RiakObject value) throws IOException {
		store(value, null);
	}

	public RiakObject[] store(RiakObject value, IRequestMeta meta)
			throws IOException {

		RPB.RpbPutReq.Builder builder = RPB.RpbPutReq.newBuilder().setBucket(
				value.getBucketBS()).setKey(value.getKeyBS()).setContent(
				value.buildContent());

		if (value.getVclock() != null) {
			builder.setVclock(value.getVclock());
		}

		if (meta != null) {
			meta.preparePut(builder);
		}

		RiakConnection c = getConnection();
		try {
			c.send(MSG_PutReq, builder.build());
			byte[] r = c.receive(MSG_PutResp);

			if (r == null) {
				return NO_RIAK_OBJECTS;
			}

			RpbPutResp resp = RPB.RpbPutResp.parseFrom(r);

			RiakObject[] res = new RiakObject[resp.getContentsCount()];
			ByteString vclock = resp.getVclock();

			for (int i = 0; i < res.length; i++) {
				res[i] = new RiakObject(vclock, value.getBucketBS(), value
						.getKeyBS(), resp.getContents(i));
			}

			return res;
		} finally {
			release(c);
		}
	}

	// /////////////////////

	public void delete(String bucket, String key, int rw) throws IOException {
		delete(ByteString.copyFromUtf8(bucket), ByteString.copyFromUtf8(key),
				rw);
	}

	public void delete(ByteString bucket, ByteString key, int rw)
			throws IOException {
		RpbDelReq req = RPB.RpbDelReq.newBuilder().setBucket(bucket)
				.setKey(key).setRw(rw).build();

		RiakConnection c = getConnection();
		try {
			c.send(MSG_DelReq, req);
			c.receive_code(MSG_DelResp);
		} finally {
			release(c);
		}

	}

	public void delete(String bucket, String key) throws IOException {
		delete(ByteString.copyFromUtf8(bucket), ByteString.copyFromUtf8(key));
	}

	public void delete(ByteString bucket, ByteString key) throws IOException {
		RpbDelReq req = RPB.RpbDelReq.newBuilder().setBucket(bucket)
				.setKey(key).build();

		RiakConnection c = getConnection();
		try {
			c.send(MSG_DelReq, req);
			c.receive_code(MSG_DelResp);
		} finally {
			release(c);
		}
	}

	public ByteString[] listBuckets() throws IOException {

		byte[] data;
		RiakConnection c = getConnection();
		try {
			c.send(MSG_ListBucketsReq);

			data = c.receive(MSG_ListBucketsResp);
			if (data == null) {
				return NO_BYTE_STRINGS;
			}
		} finally {
			release(c);
		}

		RpbListBucketsResp resp = RPB.RpbListBucketsResp.parseFrom(data);
		ByteString[] out = new ByteString[resp.getBucketsCount()];
		for (int i = 0; i < out.length; i++) {
			out[i] = resp.getBuckets(i);
		}
		return out;
	}

	public BucketProperties getBucketProperties(ByteString bucket)
			throws IOException {

		RiakConnection c = getConnection();
		try {
			c.send(MSG_GetBucketReq, RPB.RpbGetBucketReq.newBuilder()
					.setBucket(bucket).build());

			byte[] data = c.receive(MSG_GetBucketResp);
			BucketProperties bp = new BucketProperties();
			if (data == null) {
				return bp;
			}

			bp.init(RPB.RpbGetBucketResp.parseFrom(data));
			return bp;
		} finally {
			release(c);
		}

	}

	public void setBucketProperties(ByteString bucket, BucketProperties props)
			throws IOException {

		RPB.RpbSetBucketReq req = RPB.RpbSetBucketReq.newBuilder().setBucket(
				bucket).setProps(props.build()).build();

		RiakConnection c = getConnection();
		try {
			c.send(MSG_SetBucketReq, req);
			c.receive_code(MSG_SetBucketResp);
		} finally {
			release(c);
		}
	}

	// /////////////////////

	public KeySource listKeys(ByteString bucket) throws IOException {

		RiakConnection c = getConnection();
		c.send(MSG_ListKeysReq, RPB.RpbListKeysReq.newBuilder().setBucket(
				bucket).build());

		return new KeySource(this, c);
	}

	// /////////////////////

	public MapReduceResponseSource mapReduce(JSONObject obj) throws IOException {
		return mapReduce(ByteString.copyFromUtf8(obj.toString()),
				new RequestMeta().contentType("application/json"));
	}

	public MapReduceResponseSource mapReduce(String request,
			IRequestMeta meta) throws IOException {
		return mapReduce(ByteString.copyFromUtf8(request), meta);
	}
	
	public MapReduceResponseSource mapReduce(ByteString request,
			IRequestMeta meta) throws IOException {
		RiakConnection c = getConnection();

		ByteString contentType = meta.getContentType();
		if (contentType == null) {
			throw new IllegalArgumentException("no content type");
		}
		RpbMapRedReq req = RPB.RpbMapRedReq.newBuilder().setRequest(request)
				.setContentType(meta.getContentType()).build();

		c.send(MSG_MapRedReq, req);
		
		return new MapReduceResponseSource(this, c, contentType);
	}

}
