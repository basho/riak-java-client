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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;

import com.basho.riak.client.RiakLink;
import com.basho.riak.client.response.FetchResponse;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.HttpResponseDecorator;
import com.basho.riak.client.response.RiakResponseException;
import com.basho.riak.client.util.ClientUtils;
import com.basho.riak.client.util.Constants;

/**
 * Decorates an HttpResponse to interpret fetch and fetchMeta responses from
 * Riak's Raw interface which returns object metadata in HTTP headers and value
 * in the body.
 */
public class RawFetchResponse extends HttpResponseDecorator implements FetchResponse {

    private RawObject object = null;
    private List<RawObject> siblings = null;
    private InputStream bodyStream = null;
    private String body = null;

    /**
     * Construct a {@link RawFetchResponse} that will parse objects in an HTTP
     * response from the Riak Raw interface. For streamed responses (e.g.
     * returned by RawClient.stream(bucket, key), use getBodyAsStream(), keeping
     * in mind that the Raw interface may return sibling objects in a
     * multipart/mixed document if allow_mult is set to true on the bucket. Any
     * streamed response is automatically consumed and parsed upon calling
     * getBody(), has/getObject() or has/getSiblings().
     * 
     * @throws RiakResponseException
     *             If the server returns a 300 without a proper multipart/mixed
     *             body
     */
    public RawFetchResponse(HttpResponse r) throws RiakResponseException {
        super(r);
        
        if (r == null)
            return;
        
        int status = r.getStatusCode();
        if (status == 300) {
            String contentType = r.getHttpHeaders().get(Constants.HDR_CONTENT_TYPE);

            if (contentType == null || !(contentType.trim().toLowerCase().startsWith(Constants.CTYPE_MULTIPART_MIXED)))
                throw new RiakResponseException(r, "multipart/mixed content expected when object has siblings");
        }

        // If response was constructed without a response body, try to get the
        // body as a stream from the underlying HTTP method
        if (r.getBody() == null && r.getHttpMethod() != null) {
            try {
                bodyStream = r.getHttpMethod().getResponseBodyAsStream();
            } catch (IOException e) { /* ignore */}
        }
    }

    /**
     * @return Whether or not the HTTP response contains an object. If this is a
     *         streamed response, the stream will be consumed, parsed, and
     *         closed.
     */
    public boolean hasObject() {
        return getObject() != null;
    }

    /**
     * On a 2xx response, parse the HTTP response from the Raw interface into a
     * {@link RawObject}. If this is a streamed response, the stream will be
     * consumed, parsed, and closed.
     */
    public RawObject getObject() {
        if (object == null && isSuccess()) {
            Map<String, String> headers = getHttpHeaders();
            Collection<RiakLink> links = RawUtils.parseLinkHeader(headers.get(Constants.HDR_LINK));
            Map<String, String> usermeta = RawUtils.parseUsermeta(headers);
            object = new RawObject(getBucket(), getKey(), getBody(), links, usermeta,
                                   headers.get(Constants.HDR_CONTENT_TYPE), headers.get(Constants.HDR_VCLOCK),
                                   headers.get(Constants.HDR_LAST_MODIFIED), headers.get(Constants.HDR_ETAG));
        } else if (object == null && getStatusCode() == 300) {
            Collection<RawObject> siblings = getSiblings();
            if (siblings.size() > 0) {
                object = siblings.iterator().next();
            }
        }
        return object;
    }

    /**
     * @return Whether or not the HTTP response contains an object. If this is a
     *         streamed response, the stream will be consumed, parsed, and
     *         closed.
     */
    public boolean hasSiblings() {
        return getSiblings().size() > 0;
    }

    /**
     * On a 300 response, parse the multipart/mixed HTTP body into a list of
     * sibling {@link RawObject}s. If this is a streamed response, the stream
     * will be consumed, parsed, and closed.
     */
    public Collection<RawObject> getSiblings() {
        if (siblings == null) {
            if (getStatusCode() == 300) {
                siblings = RawUtils.parseMultipart(getBucket(), getKey(), getHttpHeaders(), getBody());
            } else {
                siblings = new ArrayList<RawObject>();
            }
        }
        return siblings;
    }

    public boolean hasStream() {
        return bodyStream != null;
    }

    /**
     * @return
     */
    public InputStream getBodyAsStream() {
        return bodyStream;
    }

    @Override
    public String getBody() {
        if (body != null)
            return body;

        body = impl.getBody();
        if (body == null && bodyStream != null) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            try {
                ClientUtils.copyStream(bodyStream, os);
                body = new String(os.toByteArray());
            } catch (IOException e) {
                // ignore; just return null body
            } finally {
                close();
            }
        }
        return body;
    }

    /**
     * Releases the underlying HTTP connection, closing the InputStream returned
     * by getBodyAsStream(), if any. User is responsible for calling this method
     * on RawFetchResponse objects returned from streaming requests, such as
     * RawClient.stream(bucket, key).
     */
    public void close() {
        if (bodyStream != null) {
            try {
                bodyStream.close();
            } catch (IOException e) { /* ignore */}
            bodyStream = null;
        }

        HttpMethod httpMethod = getHttpMethod();
        if (httpMethod != null) {
            httpMethod.releaseConnection();
        }
    }
}
