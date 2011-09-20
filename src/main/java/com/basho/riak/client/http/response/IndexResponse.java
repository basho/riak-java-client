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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.http.util.ClientUtils;
import com.basho.riak.client.http.util.Constants;

/**
 * The response from an index query
 * 
 * @author russell
 */
public class IndexResponse extends HttpResponseDecorator implements HttpResponse {

    private final List<String> keys;

    /**
     * @param r
     */
    public IndexResponse(HttpResponse r) throws JSONException {
        super(r);

        keys = new ArrayList<String>();

        if (r != null && r.isSuccess()) {
            Collection<String> b;
            JSONObject json = new JSONObject(r.getBodyAsString());
            JSONArray k = json.optJSONArray(Constants.FL_KEYS);
            b = ClientUtils.jsonArrayAsList(k);
            keys.addAll(b);
        }
    }

    /**
     * The list of keys returned by an index query
     * 
     * @return the keys (maybe empty, never null)
     */
    public List<String> getKeys() {
        return keys;
    }

}
