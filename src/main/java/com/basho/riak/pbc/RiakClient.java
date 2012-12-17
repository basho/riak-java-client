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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;
import org.apache.commons.codec.binary.Base64;

import org.json.JSONObject;

import com.basho.riak.client.http.util.Constants;
import com.basho.riak.client.util.CharsetUtils;
import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakPB;
import com.basho.riak.protobuf.RiakKvPB.RpbDelReq;
import com.basho.riak.protobuf.RiakKvPB.RpbGetClientIdResp;
import com.basho.riak.protobuf.RiakKvPB.RpbGetReq;
import com.basho.riak.protobuf.RiakKvPB.RpbGetResp;
import com.basho.riak.protobuf.RiakPB.RpbGetServerInfoResp;
import com.basho.riak.protobuf.RiakKvPB.RpbListBucketsResp;
import com.basho.riak.protobuf.RiakKvPB.RpbMapRedReq;
import com.basho.riak.protobuf.RiakKvPB.RpbPutReq;
import com.basho.riak.protobuf.RiakKvPB.RpbPutResp;
import com.google.protobuf.ByteString;

/**
 * Low level protocol buffers client.
 */
public class RiakClient implements RiakMessageCodes {

    private static final int BUFFER_SIZE_KB;

    static {
        BUFFER_SIZE_KB = Integer.parseInt(System.getProperty("com.basho.riak.client.pbc,buffer", "16"));
    }

    private static final RiakObject[] NO_RIAK_OBJECTS = new RiakObject[0];
	private static final ByteString[] NO_BYTE_STRINGS = new ByteString[0];
	//private static final String[] NO_STRINGS = new String[0];
	//private static final MapReduceResponse[] NO_MAP_REDUCE_RESPONSES = new MapReduceResponse[0];

	private String node;
	private String serverVersion;

	/**
	 * if this has been set (or gotten) then it will be applied to new
	 * connections
	 */
	private volatile byte[] clientId;
	private final RiakConnectionPool pool;

	public RiakClient(String host) throws IOException {
		this(host, RiakConnection.DEFAULT_RIAK_PB_PORT);
	}

	public RiakClient(String host, int port) throws IOException {
		this(InetAddress.getByName(host), port);
	}

	public RiakClient(RiakConnectionPool pool) {
	    this.pool = pool;
	}

	public RiakClient(InetAddress addr, int port) throws IOException {
		this.pool = new RiakConnectionPool(0, RiakConnectionPool.LIMITLESS, addr, port, 1000, BUFFER_SIZE_KB, 1000,0);
		this.pool.start();
	}

	public RiakClient(String host, int port, int bufferSizeKb)  throws IOException {
	    this.pool = new RiakConnectionPool(0, RiakConnectionPool.LIMITLESS, InetAddress.getByName(host), port, 1000, bufferSizeKb, 1000,0);
	    this.pool.start();
	}

	RiakConnection getConnection() throws IOException {
        RiakConnection c = pool.getConnection(clientId);
        return c;
    }

	void release(RiakConnection c) {
		pool.releaseConnection(c);
	}

	/**
	 * helper method to use a reasonable default client id
	 * beware, it caches the client id. If you call it multiple times on the same client
	 * you get the *same* id (not good for reusing a client with different ids)
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
				// Not totally secure, but doesn't need to be
				// and 100% less prone to 30 second hangs on linux jdk5
				sr.setSeed(UUID.randomUUID().getLeastSignificantBits() + new Date().getTime());
			} catch (NoSuchAlgorithmException e) {
				throw new RuntimeException(e);
			}
			byte[] data = new byte[6];
			sr.nextBytes(data);
			clid = CharsetUtils.asString(Base64.encodeBase64Chunked(data), CharsetUtils.ISO_8859_1);
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

	/**
	 * Warning: the riak client id is 4 bytes. This method silently truncates anymore bytes than that.
	 * Be aware that if you have two client Ids, "boris1" and "boris2" this method will leave you with 1 client id, "bori".
	 * Use {@link RiakClient#prepareClientID()} to generate a reasonably unique Id.
	 * @see RiakClient#prepareClientID()
	 * @param id
	 * @throws IOException
	 */
	public void setClientID(String id) throws IOException {
	    if(id == null || id.length() < Constants.RIAK_CLIENT_ID_LENGTH) {
	        throw new IllegalArgumentException("Client ID must be at least " + Constants.RIAK_CLIENT_ID_LENGTH + " bytes long");
	    }
		setClientID(ByteString.copyFrom(CharsetUtils.utf8StringToBytes(id), 0, Constants.RIAK_CLIENT_ID_LENGTH));
	}

	// /////////////////////

	public void setClientID(ByteString id) throws IOException {
	    if(id.size() > Constants.RIAK_CLIENT_ID_LENGTH) {
	        id = ByteString.copyFrom(id.toByteArray(), 0, Constants.RIAK_CLIENT_ID_LENGTH);
	    }

		this.clientId = id.toByteArray();
	}

	public String getClientID() throws IOException {
		RiakConnection c = getConnection();
		try {
			c.send(MSG_GetClientIdReq);
			byte[] data = c.receive(MSG_GetClientIdResp);
			if (data == null)
				return null;
			RpbGetClientIdResp res = RiakKvPB.RpbGetClientIdResp.parseFrom(data);
			clientId = res.getClientId().toByteArray();
			return CharsetUtils.asUTF8String(clientId);
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

			RpbGetServerInfoResp res = RiakPB.RpbGetServerInfoResp.parseFrom(data);
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
		RpbGetReq req = RiakKvPB.RpbGetReq.newBuilder().setBucket(bucket)
				.setKey(key).setR(readQuorum).build();

		RiakConnection c = getConnection();
		try {
			c.send(MSG_GetReq, req);
			return processFetchReply(c, bucket, key).getObjects();
		} finally {
			release(c);
		}

	}

    // All the fetch parameters
    public FetchResponse fetch(String bucket, String key, FetchMeta fetchMeta) throws IOException {
        return fetch(ByteString.copyFromUtf8(bucket), ByteString.copyFromUtf8(key), fetchMeta);
    }

    // All the fetch parameters
    public FetchResponse fetch(ByteString bucket, ByteString key, FetchMeta fetchMeta) throws IOException {
        RpbGetReq.Builder b = RiakKvPB.RpbGetReq.newBuilder().setBucket(bucket).setKey(key);
        fetchMeta.write(b);
        RiakConnection c = getConnection();

        try {
            c.send(MSG_GetReq, b.build());
            return processFetchReply(c, bucket, key);
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
		RpbGetReq req = RiakKvPB.RpbGetReq.newBuilder().setBucket(bucket)
				.setKey(key).build();

		RiakConnection c = getConnection();
		try {
			c.send(MSG_GetReq, req);
			return processFetchReply(c, bucket, key).getObjects();
		} finally {
			release(c);
		}
	}

	private FetchResponse processFetchReply(RiakConnection c, ByteString bucket, ByteString key) throws IOException {
	    byte[] rep = c.receive(MSG_GetResp);

        if (rep == null) {
            return new FetchResponse(NO_RIAK_OBJECTS, false, null);
        }

        RpbGetResp resp = RiakKvPB.RpbGetResp.parseFrom(rep);
        int count = resp.getContentCount();
        RiakObject[] out = new RiakObject[count];
        ByteString vclock = resp.getVclock();
        for (int i = 0; i < count; i++) {
            out[i] = new RiakObject(vclock, bucket, key, resp.getContent(i));
        }

        boolean unchanged = resp.getUnchanged();

        return new FetchResponse(out, unchanged, vclock.toByteArray());
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

				RiakKvPB.RpbPutReq.Builder builder = RiakKvPB.RpbPutReq.newBuilder()
						.setBucket(value.getBucketBS())
						.setKey(value.getKeyBS()).setContent(
								value.buildContent());

				if (value.getVclock() != null) {
					builder.setVclock(value.getVclock());
				}

				builder.setReturnBody(true);

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
						RpbPutResp resp = RiakKvPB.RpbPutResp.parseFrom(data);
						if (resp.hasVclock()) {
							vclocks[i] = resp.getVclock();
						}
					}
				}
			} catch (IOException e) {
				// TODO at least log it
				e.printStackTrace();
			}

		}

	}

	public void store(RiakObject value) throws IOException {
		store(value, null);
	}

	public RiakObject[] store(RiakObject value, IRequestMeta meta)
			throws IOException {

		RiakKvPB.RpbPutReq.Builder builder = RiakKvPB.RpbPutReq.newBuilder().setBucket(
				value.getBucketBS()).setContent(
				value.buildContent());

		if (value.getKeyBS() != null) {
			builder.setKey(value.getKeyBS());
		}

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

			RpbPutResp resp = RiakKvPB.RpbPutResp.parseFrom(r);

			RiakObject[] res = new RiakObject[resp.getContentCount()];
			ByteString vclock = resp.getVclock();

			// The key parameter will be set only if the server generated a 
			// key for the object so we check and set it accordingly
			for (int i = 0; i < res.length; i++) {
				res[i] = new RiakObject(vclock, value.getBucketBS(), 
                    (resp.hasKey()) ? resp.getKey() : value.getKeyBS(), 
                    resp.getContent(i));
			}

			return res;
		} finally {
			release(c);
		}
	}

	// /////////////////////

	public void delete(String bucket, String key, DeleteMeta deleteMeta) throws IOException {
	    delete(ByteString.copyFromUtf8(bucket), ByteString.copyFromUtf8(key), deleteMeta);
	}

	public void delete(ByteString bucket, ByteString key, DeleteMeta deleteMeta) throws IOException {
	    RpbDelReq.Builder builder = RiakKvPB.RpbDelReq.newBuilder().setBucket(bucket)
        .setKey(key);

	    deleteMeta.write(builder);

	    RiakConnection c = getConnection();

	    try {
            c.send(MSG_DelReq, builder.build());
            c.receive_code(MSG_DelResp);
        } finally {
            release(c);
        }

    }

	public void delete(String bucket, String key, int rw) throws IOException {
		delete(ByteString.copyFromUtf8(bucket), ByteString.copyFromUtf8(key),
				rw);
	}

	public void delete(ByteString bucket, ByteString key, int rw)
			throws IOException {
		RpbDelReq req = RiakKvPB.RpbDelReq.newBuilder().setBucket(bucket)
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
		RpbDelReq req = RiakKvPB.RpbDelReq.newBuilder().setBucket(bucket)
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

		RpbListBucketsResp resp = RiakKvPB.RpbListBucketsResp.parseFrom(data);
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
			c.send(MSG_GetBucketReq, RiakKvPB.RpbGetBucketReq.newBuilder()
					.setBucket(bucket).build());

			byte[] data = c.receive(MSG_GetBucketResp);
			BucketProperties bp = new BucketProperties();
			if (data == null) {
				return bp;
			}

			bp.init(RiakKvPB.RpbGetBucketResp.parseFrom(data));
			return bp;
		} finally {
			release(c);
		}

	}

	public void setBucketProperties(ByteString bucket, BucketProperties props)
			throws IOException {

		RiakKvPB.RpbSetBucketReq req = RiakKvPB.RpbSetBucketReq.newBuilder().setBucket(
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
		c.send(MSG_ListKeysReq, RiakKvPB.RpbListKeysReq.newBuilder().setBucket(
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
		RpbMapRedReq req = RiakKvPB.RpbMapRedReq.newBuilder().setRequest(request)
				.setContentType(meta.getContentType()).build();

		c.send(MSG_MapRedReq, req);
		
		return new MapReduceResponseSource(this, c, contentType);
	}

  public void shutdown()
  {
    pool.shutdown();
  }

}
