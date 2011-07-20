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
package com.basho.riak.client.plain;

import java.io.InputStream;
import java.util.Map;

import org.apache.http.client.methods.HttpRequestBase;

import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.RiakResponseRuntimeException;

/**
 * A checked decorator for {@link RiakResponseRuntimeException}
 *
 * @deprecated with the addition of a protocol buffers client in 0.14 all the
 *             existing REST client code should be in client.http.* this class
 *             has therefore been moved. Please use
 *             com.basho.riak.client.http.plain.RiakResponseException
 *             instead.
 *             <p>WARNING: This class will be REMOVED in the next version.</p>
 * @see com.basho.riak.client.http.plain.RiakResponseException
 */
@Deprecated
public class RiakResponseException extends Exception implements HttpResponse {

    private static final long serialVersionUID = 5932513075276473483L;
    private RiakResponseRuntimeException impl;

    public RiakResponseException(RiakResponseRuntimeException e) {
        super(e.getMessage(), e.getCause());
        impl = e;
    }

    public byte[] getBody() {
        return impl.getBody();
    }
    
    public String getBodyAsString() {
       return impl.getBodyAsString();
    }

    public InputStream getStream() {
        return impl.getStream();
    }

    public boolean isStreamed() {
        return impl.isStreamed();
    }
    
    public String getBucket() {
        return impl.getBucket();
    }

    public Map<String, String> getHttpHeaders() {
        return impl.getHttpHeaders();
    }

    public HttpRequestBase getHttpMethod() {
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

    public void close() {
        impl.close();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.http.response.HttpResponse#getHttpResponse()
     */
    public org.apache.http.HttpResponse getHttpResponse() {
        return impl.getHttpResponse();
    }
}
