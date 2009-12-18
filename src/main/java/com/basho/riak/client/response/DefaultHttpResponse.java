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
import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;

import com.basho.riak.client.util.ClientUtils;
import com.basho.riak.client.util.Constants;

/**
 * Simple implementation of HttpResponse interface. Simply stores and returns
 * the various fields.
 */
public class DefaultHttpResponse implements HttpResponse {

    private String bucket;
    private String key;
    private int status = -1;
    private Map<String, String> headers = null;
    private String body = null;
    private HttpMethod httpMethod = null;

    public DefaultHttpResponse(String bucket, String key, int status, Map<String, String> headers, String body,
            HttpMethod httpMethod) {
        this.bucket = bucket;
        this.key = key;
        this.status = status;
        this.headers = headers;
        this.body = body;
        this.httpMethod = httpMethod;
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }

    public int getStatusCode() {
        return status;
    }

    public Map<String, String> getHttpHeaders() {
        return headers;
    }

    public String getBody() {
        return body;
    }

    public HttpMethod getHttpMethod() {
        return httpMethod;
    }

    public boolean isSuccess() {
        return (status >= 200 && status < 300) ||
               (status == 404 && Constants.HTTP_DELETE_METHOD.equals(httpMethod.getName())) ||
               (status == 304 && (Constants.HTTP_HEAD_METHOD.equals(httpMethod.getName()) || Constants.HTTP_GET_METHOD.equals(httpMethod.getName())));
    }

    public boolean isError() {
        return status >= 400;
    }

    /**
     * Construct an {@link DefaultHttpResponse} from an {@link HttpMethod}.
     * 
     * @param bucket
     *            Target object's bucket
     * @param key
     *            Target object's key or null if bucket is target
     * @param httpMethod
     *            HttpMethod containing response from Riak
     * @return {@link DefaultHttpResponse} containing details from response
     *         stored in <code>httpMethod</code>
     * @throws IOException
     *             if the response body cannot be retrieved from
     *             <code>httpMethod</code> (i.e.
     *             HttpMethod.getResponseBodyAsString() throws an IOException)
     */
    public static DefaultHttpResponse fromHttpMethod(String bucket, String key, final HttpMethod httpMethod)
            throws IOException {
        int status = httpMethod.getStatusCode();
        Map<String, String> headers = ClientUtils.asHeaderMap(httpMethod.getResponseHeaders());
        String body = httpMethod.getResponseBodyAsString();
        return new DefaultHttpResponse(bucket, key, status, headers, body, httpMethod);
    }
}
