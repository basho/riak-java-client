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

package com.basho.riak.pbc.mapreduce;

import org.json.JSONArray;
import org.json.JSONException;

import com.basho.riak.pbc.RPB.RpbMapRedResp;
import com.google.protobuf.ByteString;

/**
 * The results of executing a map/reduce query, wraps the PBC message response
 */
public class MapReduceResponse {

	static final ByteString APPLICATION_JSON = ByteString.copyFromUtf8("application/json");
	
	public final Integer phase;
	public final ByteString response;
	private ByteString contentType;

	public MapReduceResponse(RpbMapRedResp resp, ByteString contentType2) {
		if (resp.hasPhase()) {
			this.phase = new Integer(resp.getPhase());
		} else {
			this.phase = null;
		}
		if (resp.hasResponse()) {
			this.response = resp.getResponse();
		} else {
			this.response = null;
		}
		this.contentType = contentType2;
	}

	public JSONArray getJSON() throws JSONException {
		if (response == null)
			return null;
		if (!APPLICATION_JSON.equals(contentType))
			throw new IllegalArgumentException("not application/json");
		return new JSONArray(response.toStringUtf8());
	}

	public ByteString getContent() {
		if (response == null)
			return null;
		return response;
	}

	public String getContentType() {
		return contentType.toStringUtf8();
	}
}
