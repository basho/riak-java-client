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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;

import com.basho.riak.client.RiakLink;
import com.basho.riak.client.response.FetchResponse;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.RiakResponseException;
import com.basho.riak.client.util.Constants;

/**
 * Decorates an HttpResponse to interpret fetch and fetchMeta responses from
 * Riak's Raw interface which returns object metadata in HTTP headers and value
 * in the body.
 */
public class RawFetchResponse implements FetchResponse {

    private HttpResponse impl;
    private RawObject object;
    private List<RawObject> siblings = new ArrayList<RawObject>();

    /**
     * On a 2xx response, parse the HTTP response from the Raw interface into a
     * {@link RiakObject}. On a 300 response, parse the multipart/mixed HTTP
     * body into a list of sibling {@link RiakObject}s.
     */
    public RawFetchResponse(HttpResponse r) {
        Map<String, String> headers = r.getHttpHeaders();
        Collection<RiakLink> links = RawUtils.parseLinkHeader(headers.get(Constants.HDR_LINK));
        Map<String, String> usermeta = RawUtils.parseUsermeta(headers);

        impl = r;

        if (r.getStatusCode() == 300) {
            String contentType = headers.get(Constants.HDR_CONTENT_TYPE);

            if (contentType == null || !(contentType.trim().toLowerCase().startsWith(Constants.CTYPE_MULTIPART_MIXED)))
                throw new RiakResponseException(r, "multipart/mixed content expected when object has siblings");

            siblings = RawUtils.parseMultipart(r.getBucket(), r.getKey(), headers, r.getBody());
            if (siblings.size() > 0) {
                object = siblings.get(0);
            }
        } else if (r.isSuccess()) {
            object = new RawObject(r.getBucket(), r.getKey(), r.getBody(), links, usermeta,
                                   headers.get(Constants.HDR_CONTENT_TYPE), headers.get(Constants.HDR_VCLOCK),
                                   headers.get(Constants.HDR_LAST_MODIFIED), headers.get(Constants.HDR_ETAG));
        }
    }

    public boolean hasObject() {
        return object != null;
    }

    public RawObject getObject() {
        return object;
    }

    public boolean hasSiblings() {
        return siblings.size() > 0;
    }

    public Collection<RawObject> getSiblings() {
        return Collections.unmodifiableCollection(siblings);
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
