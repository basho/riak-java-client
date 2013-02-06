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
package com.basho.riak.client.http.response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.basho.riak.client.http.RiakClient;
import com.basho.riak.client.http.RiakIndex;
import com.basho.riak.client.http.RiakLink;
import com.basho.riak.client.http.RiakObject;
import com.basho.riak.client.http.util.ClientUtils;
import com.basho.riak.client.http.util.Constants;
import com.basho.riak.client.http.util.StreamedMultipart;

/**
 * Response from a HEAD or GET request for an object. Decorates an HttpResponse
 * to interpret fetch and fetchMeta responses from Riak's HTTP interface which
 * returns object metadata in HTTP headers and value in the body.
 */
public class FetchResponse extends HttpResponseDecorator implements WithBodyResponse {

    private RiakObject object = null;
    private Collection<RiakObject> siblings = new ArrayList<RiakObject>();
    private String vclock;

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
     *             body or the server returns a 400 Bad Request or 5xx failure
     * @throws RiakIORuntimeException
     *             If an error occurs during communication with the Riak server.
     */
    public FetchResponse(HttpResponse r, RiakClient riak) throws RiakResponseRuntimeException, RiakIORuntimeException {
        super(r);

        if (r == null)
            return;

        Map<String, String> headers = r.getHttpHeaders();
        List<RiakLink> links = ClientUtils.parseLinkHeader(headers.get(Constants.HDR_LINK));
        @SuppressWarnings("rawtypes") List<RiakIndex> indexes = ClientUtils.parseIndexHeaders(headers);
        Map<String, String> usermeta = ClientUtils.parseUsermeta(headers);
        vclock = headers.get(Constants.HDR_VCLOCK);

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
            
            // There is a bug in Riak where the x-riak-deleted header is not returned
            // with a tombstone on a 404 (x-riak-vclock exists). The following block can
            // be removed once that is fixed
            byte[] body = r.getBody();
            if (r.getStatusCode() == 404) {
                headers.put(Constants.HDR_DELETED, "true");
                body = new byte[0]; // otherwise this will be "not found"
            }
            
            
            object = new RiakObject(riak, r.getBucket(), r.getKey(), body,
                                    headers.get(Constants.HDR_CONTENT_TYPE), links, usermeta,
                                    headers.get(Constants.HDR_VCLOCK), headers.get(Constants.HDR_LAST_MODIFIED),
                                    headers.get(Constants.HDR_ETAG), indexes, 
                                    headers.get(Constants.HDR_DELETED) != null ? true : false);

            Long contentLength = null;
            try {
                contentLength = Long.parseLong(headers
                        .get(Constants.HDR_CONTENT_LENGTH));
            } catch (NumberFormatException ignored) {}
        
            object.setValueStream(r.getStream(), contentLength);
        } else if (r.isError() && (r.getStatusCode() != 404) && (r.getStatusCode() != 412)) {
            // 404 and 412 are allowed to pass through due to upsream requirements
            throw new RiakResponseRuntimeException(r, r.getBodyAsString());
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

    /**
     * @return the X-Riak-Vclock header value, if present.
     */
    public String getVclock() {
        return vclock;
    }
}
