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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;
import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.response.FetchResponse;
import com.basho.riak.client.response.HttpResponse;

/**
 * Decorates an HttpResponse to interpret fetch and fetchMeta responses from
 * Jiak's Raw interface which returns object metadata and value directly in the
 * message body as JSON.
 */
public class JiakFetchResponse implements FetchResponse {

    private HttpResponse impl;

    public JiakObject getObject() {
        return object;
    }

    public boolean hasObject() {
        return object != null;
    }

    private JiakObject object;

    /**
     * On a 2xx response, parse the JSON in the body into a {@link JiakObject}.
     */
    public JiakFetchResponse(HttpResponse r) throws JSONException {
        impl = r;
        if (r.isSuccess() && (r.getBody() != null)) {
            object = new JiakObject(new JSONObject(r.getBody()));
        }
    }

    /**
     * @return false since Jiak interface doesn't support siblings
     */
    public boolean hasSiblings() {
        return false;
    }

    /**
     * @return The {@link JiakObject} if there is one or an empty list
     */
    public Collection<JiakObject> getSiblings() {
        return hasObject() ? Arrays.asList(getObject()) : new ArrayList<JiakObject>();
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
