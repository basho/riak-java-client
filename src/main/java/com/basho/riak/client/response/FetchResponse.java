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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;

import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.util.ClientUtils;
import com.basho.riak.client.util.Constants;

/**
 * Response from a HEAD or GET request for an object. Decorates an HttpResponse
 * to interpret fetch and fetchMeta responses from Riak's HTTP interface which
 * returns object metadata in HTTP headers and value in the body.
 */
public class FetchResponse extends HttpResponseDecorator implements HttpResponse {

    private RiakObject object = null;
    private List<RiakObject> siblings = new ArrayList<RiakObject>();

    /**
     * On a 2xx response, parse the HTTP response from Riak into a
     * {@link RiakObject}. On a 300 response, parse the multipart/mixed HTTP
     * body into a list of sibling {@link RiakObject}s.
     * 
     * A 2xx response is "streaming" if it has a null body and non-null stream.
     * The resulting {@link RiakObject} will return null for getValue() and the
     * stream for getValueStream(). Users must remember to release the return
     * value's underlying stream by calling close().
     * 
     * Sibling objects are not be streamed, since the stream must be consumed
     * for parsing.
     * 
     * @throws RiakResponseRuntimeException
     *             If the server returns a 300 without a proper multipart/mixed
     *             body
     */
    public FetchResponse(HttpResponse r) throws RiakResponseRuntimeException {
        super(r);

        if (r == null)
            return;

        Map<String, String> headers = r.getHttpHeaders();
        List<RiakLink> links = ClientUtils.parseLinkHeader(headers.get(Constants.HDR_LINK));
        Map<String, String> usermeta = ClientUtils.parseUsermeta(headers);

        if (r.getStatusCode() == 300) {
            String contentType = headers.get(Constants.HDR_CONTENT_TYPE);

            if (contentType == null || !(contentType.trim().toLowerCase().startsWith(Constants.CTYPE_MULTIPART_MIXED)))
                throw new RiakResponseRuntimeException(r, "multipart/mixed content expected when object has siblings");

            String body = r.getBody();

            // If response is streamed, consume and close the stream
            if (r.getBody() == null && r.getHttpMethod() != null) {
                try {
                    ByteArrayOutputStream os = new ByteArrayOutputStream();
                    ClientUtils.copyStream(r.getHttpMethod().getResponseBodyAsStream(), os);
                    body = os.toString();
                } catch (IOException e) { // ignore
                } finally {
                    close();
                }
            }

            siblings = ClientUtils.parseMultipart(r.getBucket(), r.getKey(), headers, body);
            if (siblings.size() > 0) {
                object = siblings.get(0);
            }
        } else if (r.isSuccess()) {
            object = new RiakObject(r.getBucket(), r.getKey(), r.getBody(), headers.get(Constants.HDR_CONTENT_TYPE),
                                    links, usermeta, headers.get(Constants.HDR_VCLOCK),
                                    headers.get(Constants.HDR_LAST_MODIFIED), headers.get(Constants.HDR_ETAG));

            // If response was constructed without a response body, try to get
            // the body as a stream from the underlying HTTP method
            if (r.getBody() == null && r.getHttpMethod() != null) {
                Long contentLength = null;
                try {
                    contentLength = Long.parseLong(headers.get(Constants.HDR_CONTENT_LENGTH));
                } catch (NumberFormatException ignored) {}
                try {
                    object.setValueStream(r.getHttpMethod().getResponseBodyAsStream(), contentLength);
                } catch (IOException ignored) {}
            }
        }
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
     * Releases the underlying HTTP connection, closing the InputStream returned
     * by getObject().getValueStream(), if any. User is responsible for calling
     * this method on FetchResponse objects returned from streaming requests,
     * such as RiakClient.stream(bucket, key).
     */
    public void close() {
        HttpMethod httpMethod = getHttpMethod();
        if (httpMethod != null) {
            httpMethod.releaseConnection();
        }
    }
}
