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
package com.basho.riak.client.http.response;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.http.util.ClientUtils;
import com.basho.riak.client.http.util.Constants;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.json.JSONTokener;

/**
 * @author russell
 * 
 */
public class ListBucketsResponse extends HttpResponseDecorator implements HttpResponse {

    private Collection<String> buckets;

    /**
     * Create a list buckets response that parses the resppns ebody json into a
     * set of buckets.
     * 
     * @param r
     */
    public ListBucketsResponse(final HttpResponse r) throws JSONException, IOException {
        super(r);

        if (r != null && r.isSuccess()) {
            JSONObject json;
            Collection<String> b;
            if (!r.isStreamed()) {
                json = new JSONObject(r.getBodyAsString());
                JSONArray jsonBuckets = json.optJSONArray(Constants.FL_BUCKETS);
                buckets = ClientUtils.jsonArrayAsList(jsonBuckets);
            }
            else {
                InputStream stream = r.getStream();
                JSONTokener tokens = new JSONTokener(new InputStreamReader(stream));
                buckets = new StreamedBucketsCollection(tokens);
            }
        } else {
            buckets = new HashSet<String>();
        }
    }

    /**
     * Get a *copy* of the Set of buckets returned.
     * 
     * @return the buckets
     */
    public Set<String> getBuckets() {
        return new HashSet<String>(buckets);
    }

}
