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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.json.JSONArray;
import org.json.JSONException;

import com.basho.riak.pbc.RPB.RpbMapRedResp;
import com.basho.riak.pbc.mapreduce.MapReduceResponse;
import com.google.protobuf.ByteString;

/**
 * Wraps a stream of m/r responses as an iterator.
 */
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

        try {
            byte[] data = conn.receive(RiakMessageCodes.MSG_MapRedResp);
            if (data == null) {
                close();
                throw new IOException("received empty response");
            }
            r = RPB.RpbMapRedResp.parseFrom(data);
            is_given = false;
        } catch (IOException e) {
            close();
            throw e;
        }
	}

	/**
	 * Buffer the full set of results into a JSONArray
	 * @param response the {@link MapReduceResponseSource}
	 * @return a {@link JSONArray} with all phases and all results.
	 * @throws IOException
	 */
	public static JSONArray readAllResults(final MapReduceResponseSource response) throws IOException {
	    Map<Integer, JSONArray> phases = new LinkedHashMap<Integer, JSONArray>();
        MapReduceResponse mrr;
        JSONArray latest;
        int phase = 0;

        while (response.hasNext()) {
            mrr = response.next();
            try {
                latest = mrr.getJSON();

                if (latest != null) {
                    phase = mrr.getPhase();
                    JSONArray results;
                    if (phases.containsKey(phase)) {
                        results = phases.get(phase);
                    } else {
                        results = new JSONArray();
                        phases.put(phase, results);
                    }
                    for (int i = 0; i < latest.length(); i++) {
                        results.put(latest.get(i));
                    }
                }
            } catch (JSONException e) {
               throw new IOException(mrr.response.toStringUtf8(), e);
            }
        }

        JSONArray results;
        // flatten phases
        if (phases.size() == 1) {
            results = phases.get(phase);
        } else {
            results = new JSONArray();

            for (JSONArray p : phases.values()) {
                results.put(p);
            }
        }
        return results;
	}
}
