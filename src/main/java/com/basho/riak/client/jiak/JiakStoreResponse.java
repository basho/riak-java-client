/*
 * This file is provided to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain
 * a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.basho.riak.client.jiak;

import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;
import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.StoreResponse;
import com.basho.riak.client.util.Constants;

public class JiakStoreResponse implements StoreResponse {

    private HttpResponse impl;
    private JSONObject object = new JSONObject();

    public JiakStoreResponse(HttpResponse r) throws JSONException {
        impl = r;
        if (r.isSuccess()) {
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

    public String getBody() {
        return impl.getBody();
    }

    public String getBucket() {
        return impl.getBucket();
    }

    public Map<String, String> getHttpHeaders() {
        return impl.getHttpHeaders();
    }

    public HttpMethod getHttpMethod() {
        return impl.getHttpMethod();
    }

    public String getKey() {
        return impl.getKey();
    }

    public int getStatusCode() {
        return impl.getStatusCode();
    }

    public boolean isError() {
        return impl.isError();
    }

    public boolean isSuccess() {
        return impl.isSuccess();
    }

}
