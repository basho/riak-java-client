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
package com.basho.riak.client.response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.util.ClientUtils;
import com.basho.riak.client.util.Constants;
import com.basho.riak.client.util.StreamedMultipart;

/**
 * Response from a HEAD or GET request for an object. Decorates an HttpResponse
 * to interpret fetch and fetchMeta responses from Riak's HTTP interface which
 * returns object metadata in HTTP headers and value in the body.
 */
public class FetchResponse extends HttpResponseDecorator implements HttpResponse {

    private RiakObject object = null;
    private Collection<RiakObject> siblings = new ArrayList<RiakObject>();

    /**
     * On a 2xx response, parse the HTTP response from Riak into a
     * {@link RiakObject}. On a 300 response, parse the multipart/mixed HTTP
     * body into a collection of sibling {@link RiakObject}s.
     * 
     * A streaming response (i.e. r.isStreaming() == true), will have a null
     * body and non-null stream. The resulting {@link RiakObject}(s) will return
     * null for getValue() and the stream for getValueStream(). Users must
     * remember to release the return value's underlying stream by calling
     * close().
     * 
     * Sibling objects are also streamed. The values of the objects are buffered
     * in memory as the stream is read. Consume and/or close each
     * {@link RiakObject}'s stream as the collection is iterated to allow the
     * buffers to be freed.
     * 
     * @throws RiakResponseRuntimeException
     *             If the server returns a 300 without a proper multipart/mixed
     *             body
     * @throws RiakIORuntimeException
     *             If an error occurs during communication with the Riak server.
     */
    public FetchResponse(HttpResponse r, RiakClient riak) throws RiakResponseRuntimeException, RiakIORuntimeException {
        super(r);

        if (r == null)
            return;

        Map<String, String> headers = r.getHttpHeaders();
        List<RiakLink> links = ClientUtils.parseLinkHeader(headers.get(Constants.HDR_LINK));
        Map<String, String> usermeta = ClientUtils.parseUsermeta(headers);

        if (r.getStatusCode() == 300) {
            String contentType = headers.get(Constants.HDR_CONTENT_TYPE);

            if (contentType == null || !(contentType.trim().toLowerCase().startsWith(Constants.CTYPE_MULTIPART_MIXED))) {
                throw new RiakResponseRuntimeException(r, "multipart/mixed content expected when object has siblings");
            }

            if (r.isStreamed()) {
                try {
                    StreamedMultipart multipart = new StreamedMultipart(headers, r.getStream());
                    siblings = new StreamedSiblingsCollection(riak, r.getBucket(), r.getKey(), multipart);
                } catch (IOException e) {
                    throw new RiakIORuntimeException("Error finding initial boundary", e);
                }
            } else {
                siblings = ClientUtils.parseMultipart(riak, r.getBucket(), r.getKey(), headers, r.getBody());
            }

            object = siblings.iterator().next();
        } else if (r.isSuccess()) {
            object = new RiakObject(riak, r.getBucket(), r.getKey(), r.getBody(),
                                    headers.get(Constants.HDR_CONTENT_TYPE), links, usermeta,
                                    headers.get(Constants.HDR_VCLOCK), headers.get(Constants.HDR_LAST_MODIFIED),
                                    headers.get(Constants.HDR_ETAG));

            Long contentLength = null;
            try {
                contentLength = Long.parseLong(headers
                        .get(Constants.HDR_CONTENT_LENGTH));
            } catch (NumberFormatException ignored) {}
        
            object.setValueStream(r.getStream(), contentLength);
        }
    }

    public FetchResponse(HttpResponse r) throws RiakResponseRuntimeException {
        this(r, null);
    }

    /**
     * Whether response contained a Riak object
     */
    public boolean hasObject() {
        return getObject() != null;
    }

    /**
     * Returns the first Riak object contained in the response. Equivalent to
     * the first object in getSiblings() when hasSiblings() is true.
     */
    public RiakObject getObject() {
        return object;
    }

    public void setObject(RiakObject object) {
        this.object = object;
    }

    /**
     * Whether response contained a multiple Riak objects
     */
    public boolean hasSiblings() {
        return getSiblings().size() > 0;
    }

    /**
     * Returns a collection of the Riak objects contained in the response.
     */
    public Collection<RiakObject> getSiblings() {
        return siblings;
    }
}
