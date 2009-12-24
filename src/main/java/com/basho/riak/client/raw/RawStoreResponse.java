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
package com.basho.riak.client.raw;

import java.util.Map;

import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.HttpResponseDecorator;
import com.basho.riak.client.response.StoreResponse;
import com.basho.riak.client.util.Constants;

/**
 * Decorates an HttpResponse to interpret store responses from Riak's Raw
 * interface which returns updated object metadata in HTTP headers.
 */
public class RawStoreResponse extends HttpResponseDecorator implements StoreResponse {

    private String vclock = null;
    private String lastmod = null;
    private String vtag = null;

    /**
     * On a 2xx response, parses the HTTP headers into updated object metadata.
     */
    public RawStoreResponse(HttpResponse r) {
        super(r);

        if (r.isSuccess()) {
            Map<String, String> headers = r.getHttpHeaders();
            vclock = headers.get(Constants.HDR_VCLOCK);
            lastmod = headers.get(Constants.HDR_LAST_MODIFIED);
            vtag = headers.get(Constants.HDR_ETAG);
        }
    }

    public String getVclock() {
        return vclock;
    }

    public String getLastmod() {
        return lastmod;
    }

    public String getVtag() {
        return vtag;
    }
}
