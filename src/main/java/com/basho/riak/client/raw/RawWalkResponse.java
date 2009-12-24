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

import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.HttpResponseDecorator;
import com.basho.riak.client.response.RiakResponseException;
import com.basho.riak.client.response.WalkResponse;
import com.basho.riak.client.util.Constants;
import com.basho.riak.client.util.Multipart;

/**
 * Decorates an HttpResponse to interpret walk responses from Riak's Raw
 * interface which returns multipart/mixed documents.
 */
public class RawWalkResponse extends HttpResponseDecorator implements WalkResponse {

    private List<? extends List<RawObject>> steps = new ArrayList<List<RawObject>>();

    /**
     * On a 2xx response, parses the HTTP body into a list of steps. Each step
     * contains a list of objects returned in that step. The HTTP body is a
     * multipart/mixed message with multipart/mixed subparts
     */
    public RawWalkResponse(HttpResponse r) throws RiakResponseException {
        super(r);
        
        if (r != null && r.isSuccess()) {
            steps = parseSteps(r);
        }
    }

    public boolean hasSteps() {
        return steps.size() > 0;
    }

    public List<? extends List<RawObject>> getSteps() {
        return steps;
    }

    /**
     * Parse a multipart/mixed message with multipart/mixed subparts into a list
     * of lists.
     * 
     * @param r
     *            HTTP response from the Riak Raw interface
     * @return A list of lists of {@link RawObject}s represented by the
     *         response.
     * @throws RiakResponseException
     *             If one of the parts of the body doesn't contain a proper
     *             multipart/mixed message
     */
    private static List<? extends List<RawObject>> parseSteps(HttpResponse r) throws RiakResponseException {
        String bucket = r.getBucket();
        String key = r.getKey();
        List<List<RawObject>> parsedSteps = new ArrayList<List<RawObject>>();
        List<Multipart.Part> parts = Multipart.parse(r.getHttpHeaders(), r.getBody());

        if (parts != null) {
            for (Multipart.Part part : parts) {
                Map<String, String> partHeaders = part.getHeaders();
                String contentType = partHeaders.get(Constants.HDR_CONTENT_TYPE);
    
                if (contentType == null || !(contentType.trim().toLowerCase().startsWith(Constants.CTYPE_MULTIPART_MIXED)))
                    throw new RiakResponseException(r, "multipart/mixed subparts expected in link walk results");
    
                parsedSteps.add(RawUtils.parseMultipart(bucket, key, partHeaders, part.getBody()));
            }
        }

        return parsedSteps;
    }
}
