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

import static com.basho.riak.client.util.CharsetUtils.asString;
import static com.basho.riak.client.util.CharsetUtils.getCharset;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.util.EntityUtils;

import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.RiakIORuntimeException;
import com.basho.riak.client.util.Constants;

/**
 * Simple implementation of HttpResponse interface. Simply stores and returns
 * the various fields.
 *
 * @deprecated with the addition of a protocol buffers client in 0.14 all the
 *             existing REST client code should be in client.http.* this class
 *             has therefore been moved. Please use
 *             com.basho.riak.client.http.response.DefaultHttpResponse
 *             instead.
 *             <p>WARNING: This class will be REMOVED in the next version.</p>
 * @see com.basho.riak.client.http.response.DefaultHttpResponse
 */
@Deprecated
public class DefaultHttpResponse implements HttpResponse {

    private String bucket;
    private String key;
    private int status = -1;
    private Map<String, String> headers = null;
    private byte[] body = null;
    private InputStream stream = null;
    private org.apache.http.HttpResponse httpResponse = null;
    private HttpRequestBase httpMethod = null;

    public DefaultHttpResponse(String bucket, String key, int status, Map<String, String> headers, byte[] body,
            InputStream stream, org.apache.http.HttpResponse httpResponse, HttpRequestBase httpMethod) {
        if (headers == null) {
            headers = new HashMap<String, String>();
        }

        this.bucket = bucket;
        this.key = key;
        this.status = status;
        this.headers = headers;
        if(body != null) {
            this.body = body.clone();
        }
        this.stream = stream;
        this.httpResponse = httpResponse;
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

    public byte[] getBody() {
        if (body != null) {
            return body.clone();
        }
        return null;
    }
    
    public String getBodyAsString() {
       if (body == null) {
          return null;
       }
       return asString(body, getCharset(headers));
    }

    public InputStream getStream() {
        return stream;
    }

    public boolean isStreamed() {
        return stream != null;
    }

    public HttpRequestBase getHttpMethod() {
        return httpMethod;
    }

    public boolean isSuccess() {
        String method = null;
        if (httpMethod != null) {
            method = httpMethod.getMethod();
        }

        return (status >= 200 && status < 300) ||
               ((status == 300 || status == 304) && Constants.HTTP_HEAD_METHOD.equals(method)) ||
               ((status == 300 || status == 304) && Constants.HTTP_GET_METHOD.equals(method)) ||
               ((status == 300) && Constants.HTTP_PUT_METHOD.equals(method)) ||
               ((status == 404) && Constants.HTTP_DELETE_METHOD.equals(method));
    }

    public boolean isError() {
        String method = null;
        if (httpResponse != null) {
            method = httpMethod.getMethod();
        }

        return (status < 100 || status >= 400) && !((status == 404) && Constants.HTTP_DELETE_METHOD.equals(method));
    }

    public void close() {
        if (httpResponse != null) {
            try {
                EntityUtils.consume(httpResponse.getEntity());
            } catch (IOException e) {
                throw new RiakIORuntimeException(e);
            }
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.http.response.HttpResponse#getHttpResponse()
     */
    public org.apache.http.HttpResponse getHttpResponse() {
        return httpResponse;
    }
}
