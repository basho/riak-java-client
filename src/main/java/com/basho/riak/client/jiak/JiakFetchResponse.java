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
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.response.FetchResponse;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.HttpResponseDecorator;

/**
 * Decorates an HttpResponse to interpret fetch and fetchMeta responses from
 * Jiak's Raw interface which returns object metadata and value directly in the
 * message body as JSON.
 */
public class JiakFetchResponse extends HttpResponseDecorator implements FetchResponse {

    private JiakObject object;

    /**
     * On a 2xx response, parse the JSON in the body into a {@link JiakObject}.
     */
    public JiakFetchResponse(HttpResponse r) throws JSONException {
        super(r);

        if (r != null && r.isSuccess() && (r.getBody() != null)) {
            object = new JiakObject(new JSONObject(r.getBody()));
        }
    }

    public boolean hasObject() {
        return object != null;
    }

    public JiakObject getObject() {
        return object;
    }

    /**
     * Returns false since Jiak interface doesn't support siblings
     */
    public boolean hasSiblings() {
        return false;
    }

    /**
     * Returns an empty list since Jiak interface doesn't support siblings
     */
    public List<JiakObject> getSiblings() {
        return new ArrayList<JiakObject>();
    }
}
