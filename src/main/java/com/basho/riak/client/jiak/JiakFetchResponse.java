/*
This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.  
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

public class JiakFetchResponse implements FetchResponse {
    
    private HttpResponse impl;

    public JiakObject getObject() { return this.object; }
    public boolean hasObject() { return this.object != null; }
    private JiakObject object;

    public JiakFetchResponse(HttpResponse r) throws JSONException {
        this.impl = r;
        if (r.isSuccess())
            this.object = new JiakObject(new JSONObject(r.getBody()));
    }

    /**
     * Returns false since Jiak interface doesn't support siblings
     * 
     * @return false
     */
    public boolean hasSiblings() {
        return false; 
    }
    
    /**
     * @return The {@link JiakObject} if there is one or an empty list
     */
    public Collection<JiakObject> getSiblings() {
        return hasObject() ?
                Arrays.asList(getObject()) :
                new ArrayList<JiakObject>();
    }

    public String getBody() { return impl.getBody(); }
    public String getBucket() { return impl.getBucket(); }
    public Map<String, String> getHttpHeaders() { return impl.getHttpHeaders(); }
    public HttpMethod getHttpMethod() { return impl.getHttpMethod(); } 
    public String getKey() { return impl.getKey(); }
    public int getStatusCode() { return impl.getStatusCode(); }
    public boolean isError() { return impl.isError(); }
    public boolean isSuccess() { return impl.isSuccess(); }
}
