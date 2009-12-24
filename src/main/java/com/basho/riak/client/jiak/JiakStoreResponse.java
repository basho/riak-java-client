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
package com.basho.riak.client.jiak;

import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.HttpResponseDecorator;
import com.basho.riak.client.response.StoreResponse;
import com.basho.riak.client.util.Constants;

/**
 * Decorates an HttpResponse to interpret store responses from the Jiak
 * interface which should returns the entire object with metadata on a store.
 * (i.e. {@link JiakClient} sends the returnbody=true query parameter on PUTs).
 */
public class JiakStoreResponse extends HttpResponseDecorator implements StoreResponse {

    private JSONObject object = new JSONObject();

    /**
     * On a 2xx response, parses the HTTP body into a JSON object.
     */
    public JiakStoreResponse(HttpResponse r) throws JSONException {
        super(r);
        if (r != null && r.isSuccess() && (r.getBody() != null)) {
            object = new JSONObject(r.getBody());
        }
    }

    public String getVclock() {
        return object.optString(Constants.JIAK_FL_VCLOCK);
    }

    public String getLastmod() {
        return object.optString(Constants.JIAK_FL_LAST_MODIFIED);
    }

    public String getVtag() {
        return object.optString(Constants.JIAK_FL_VTAG);
    }
}
