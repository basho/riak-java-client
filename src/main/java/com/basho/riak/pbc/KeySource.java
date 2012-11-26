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
import java.util.NoSuchElementException;

import com.basho.riak.protobuf.RiakKvPB.RpbListKeysResp;
import com.google.protobuf.ByteString;

/**
 * Wraps a stream of keys as an iterator.
 */
public class KeySource extends RiakStreamClient<ByteString> {

	private RpbListKeysResp r;
	private int i;

	public KeySource(RiakClient client, RiakConnection conn) throws IOException {
		super(client, conn);
		get_next_response();
	}

	public boolean hasNext() throws IOException {
		if (isClosed()) {
			return false;
		}

		if (r_is_exhausted()) {
			get_next_response();
		}

		return !isClosed();
	}

	private boolean r_is_exhausted() {
		return i == r.getKeysCount();
	}

	public ByteString next() throws IOException {
		if (!hasNext())
			throw new NoSuchElementException();
		return r.getKeys(i++);
	}

	private void get_next_response() throws IOException {
		if (isClosed())
			return;

		// either we're in the first call (r == null)
		// or we got here because we ran out of keys.
		assert r == null || r_is_exhausted();

		do {

			if (r != null) {
				if (r.hasDone() && r.getDone()) {
					close();
					return;
				}
			}

            try {
                byte[] data = conn.receive(RiakMessageCodes.MSG_ListKeysResp);
                if (data == null) {
                    close();
                    throw new IOException("received empty response");
                }

                r = com.basho.riak.protobuf.RiakKvPB.RpbListKeysResp.parseFrom(data);
                i = 0;
            } catch (IOException e) {
                close();
                throw e;
            }

			// did we got an empty chunk! get another one.
		} while (r.getKeysCount() == 0);
	}

}
