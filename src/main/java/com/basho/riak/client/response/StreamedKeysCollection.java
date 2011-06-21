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
package com.basho.riak.client.response;

import org.json.JSONException;
import org.json.JSONTokener;

import com.basho.riak.client.util.CollectionWrapper;

/**
 * Presents the stream of keys from a Riak bucket response with query parameter
 * keys=stream as a collection. Keys are read from the stream as needed. Note,
 * this class is NOT thread-safe!
 *
 * @deprecated with the addition of a protocol buffers client in 0.14 all the
 *             existing REST client code should be in client.http.* this class
 *             has therefore been moved. Please use
 *             com.basho.riak.client.http.response.StreamedKeysCollection
 *             instead.
 *             <p>WARNING: This class will be REMOVED in the next version.</p>
 * @see com.basho.riak.client.http.response.StreamedKeysCollection
 */
@Deprecated
public class StreamedKeysCollection extends CollectionWrapper<String> {

    JSONTokener tokens;
    boolean readingArray = false;

    public StreamedKeysCollection(JSONTokener tokens) {
        this.tokens = tokens;
    }
    
    /**
     * Tries to read and cache another set of keys from the input stream. This
     * function is actually just a hacked-up implementation that finds the next
     * available array in the stream and sucks elements out of it.
     */
    @Override protected boolean cacheNext() {
        if (tokens == null)
            return false;

        try {
            while (!tokens.end()) {
                char c = tokens.nextClean();
                if ((!readingArray && c == '[') || (readingArray && c == ',')) {
                    if (tokens.nextClean() != ']') {
                        tokens.back();
                        readingArray = true;
                        cache(tokens.nextValue().toString());
                        return true;
                    }
                } else if (readingArray && c == ']') {
                    readingArray = false;
                } else if (c == '\\') {
                    tokens.nextClean(); // skip over escaped chars
                }
            }
        } catch (JSONException e) { /* nop */}
        
        return false;
    }

    @Override protected void closeBackend() {
        tokens = null;
    }
}
