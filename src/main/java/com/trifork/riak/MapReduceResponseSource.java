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

package com.trifork.riak;

import java.io.IOException;
import java.util.NoSuchElementException;

import com.google.protobuf.ByteString;
import com.trifork.riak.RPB.RpbMapRedResp;
import com.trifork.riak.mapreduce.MapReduceResponse;

public class MapReduceResponseSource extends
		RiakStreamClient<MapReduceResponse> {

	private RpbMapRedResp r;
	private boolean is_given;
	private final ByteString contentType;

	protected MapReduceResponseSource(RiakClient client, RiakConnection conn,
			ByteString contentType) throws IOException {
		super(client, conn);
		this.contentType = contentType;
		get_next_response();
	}

	@Override
	public boolean hasNext() throws IOException {
		if (isClosed()) {
			return false;
		}

		if (is_given) {
			get_next_response();
		}

		return !isClosed();
	}

	@Override
	public MapReduceResponse next() throws IOException {
		if (!hasNext())
			throw new NoSuchElementException();
		is_given = true;
		return new MapReduceResponse(r, contentType);
	}

	private void get_next_response() throws IOException {
		if (isClosed())
			return;

		// either we're in the first call (r == null)
		// or we got here because we gave the reply away.
		assert r == null || is_given;

		if (r != null && is_given) {
			if (r.hasDone() && r.getDone()) {
				close();
				return;
			}
		}

		byte[] data = conn.receive(RiakMessageCodes.MSG_MapRedResp);
		if (data == null) {
			close();
			throw new IOException("received empty response");
		}

		r = RPB.RpbMapRedResp.parseFrom(data);
		is_given = false;

	}

}
