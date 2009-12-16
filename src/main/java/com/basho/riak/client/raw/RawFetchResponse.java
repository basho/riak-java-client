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
package com.basho.riak.client.raw;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;

import com.basho.riak.client.RiakLink;
import com.basho.riak.client.response.FetchResponse;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.util.Constants;
import com.basho.riak.client.util.LinkHeader;

public class RawFetchResponse implements FetchResponse {

    private HttpResponse impl;

    public boolean hasObject() { return this.object != null; }
    public RawObject getObject() { return this.object; }
    private RawObject object;

    public boolean hasSiblings() { return this.siblings.size() > 0; }
    public Collection<RawObject> getSiblings() { return Collections.unmodifiableCollection(siblings); }
    private Collection<RawObject> siblings = new ArrayList<RawObject>(); 

    public RawFetchResponse(HttpResponse r) {
        Map<String, String> headers = r.getHttpHeaders();
        Collection<RiakLink> links = parseLinkHeader(headers.get(Constants.HDR_LINK));
        Map<String, String> usermeta = new HashMap<String, String>();
        for (String header : headers.keySet()) {
            if (header.startsWith(Constants.HDR_USERMETA_PREFIX)) {
                usermeta.put(
                        header.substring(Constants.HDR_USERMETA_PREFIX.length()), 
                        headers.get(header));
            }
        }

        this.impl = r;
        this.object = new RawObject(
                r.getBucket(), r.getKey(), r.getBody(), links, usermeta,
                headers.get(Constants.HDR_CONTENT_TYPE),
                headers.get(Constants.HDR_VCLOCK),
                headers.get(Constants.HDR_LAST_MODIFIED),
                headers.get(Constants.HDR_ETAG)
                );
    }

    public String getBody() { return impl.getBody(); }
    public String getBucket() { return impl.getBucket(); }
    public Map<String, String> getHttpHeaders() { return impl.getHttpHeaders(); }
    public HttpMethod getHttpMethod() { return impl.getHttpMethod(); } 
    public String getKey() { return impl.getKey(); }
    public int getStatusCode() { return impl.getStatusCode(); }
    public boolean isError() { return impl.isError(); }
    public boolean isSuccess() { return impl.isSuccess(); }

    private static Collection<RiakLink> parseLinkHeader(String header) {
        Collection<RiakLink> links = new ArrayList<RiakLink>();
        Map<String, Map<String, String>> parsedLinks = LinkHeader.parse(header);
        for (String url : parsedLinks.keySet()) {
            RiakLink link = parseOneLink(url, parsedLinks.get(url));
            if (link != null) {
                links.add(link);
            }
        }
        return links;
    }

    private static RiakLink parseOneLink(String url, Map<String, String> params) {
        String tag = params.get(Constants.RAW_LINK_TAG);
        if (tag != null) {
            String[] parts = url.split("/");
            if (parts.length >= 2) {
                return new RiakLink(
                        parts[parts.length-1], 
                        parts[parts.length-2],
                        tag);
            }
        }
        return null;
    }
}
