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
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;

import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.RiakResponseException;
import com.basho.riak.client.response.WalkResponse;
import com.basho.riak.client.util.Constants;
import com.basho.riak.client.util.Multipart;

/**
 * Decorates an HttpResponse to interpret walk responses from Riak's Raw
 * interface which returns multipart/mixed documents.
 */
public class RawWalkResponse implements WalkResponse {

    private HttpResponse impl = null;

    public boolean hasSteps() {
        return steps.size() > 0;
    }

    public List<? extends List<RawObject>> getSteps() {
        return steps;
    }

    private List<? extends List<RawObject>> steps = new ArrayList<List<RawObject>>();

    public RawWalkResponse(HttpResponse r) {
        if (r == null)
            return;

        impl = r;
        if (r.isSuccess()) {
            steps = parseSteps(r);
        }
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

    private static List<? extends List<RawObject>> parseSteps(HttpResponse r) {
        String bucket = r.getBucket();
        String key = r.getKey();
        List<List<RawObject>> parsedSteps = new ArrayList<List<RawObject>>();
        List<Multipart.Part> parts = Multipart.parse(r.getHttpHeaders(), r.getBody());

        for (Multipart.Part part : parts) {
            Map<String, String> partHeaders = part.getHeaders();
            String contentType = partHeaders.get(Constants.HDR_CONTENT_TYPE);

            if (contentType == null || !(contentType.trim().toLowerCase().startsWith(Constants.CTYPE_MULTIPART_MIXED)))
                throw new RiakResponseException(r, "multipart/mixed content expected when object has siblings");

            parsedSteps.add(RawUtils.parseMultipart(bucket, key, partHeaders, part.getBody()));
        }

        return parsedSteps;
    }
}
