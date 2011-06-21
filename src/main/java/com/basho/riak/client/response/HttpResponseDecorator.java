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

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;

/**
 * A default decorator implementation for HttpResponse
 *
 * @deprecated with the addition of a protocol buffers client in 0.14 all the
 *             existing REST client code should be in client.http.* this class
 *             has therefore been moved. Please use
 *             com.basho.riak.client.http.response.HttpResponseDecorator
 *             instead.
 *             <p>WARNING: This class will be REMOVED in the next version.</p>
 * @see com.basho.riak.client.http.response.HttpResponseDecorator
 */
@Deprecated
public class HttpResponseDecorator implements HttpResponse {

    protected HttpResponse impl = null;

    public HttpResponseDecorator(HttpResponse r) {
        impl = r;
    }

    public String getBucket() {
        if (impl == null)
            return null;
        return impl.getBucket();
    }

    public String getKey() {
        if (impl == null)
            return null;
        return impl.getKey();
    }

    public byte[] getBody() {
        if (impl == null)
            return null;
        return impl.getBody();
    }
    
    public String getBodyAsString() {
       if (impl == null)
          return null;
       return impl.getBodyAsString();
    }

    public InputStream getStream() {
        if (impl == null)
            return null;
        return impl.getStream();
    }

    public boolean isStreamed() {
        if (impl == null)
            return false;
        return impl.isStreamed();
    }

    public Map<String, String> getHttpHeaders() {
        if (impl == null)
            return new HashMap<String, String>();
        return impl.getHttpHeaders();
    }

    public HttpMethod getHttpMethod() {
        if (impl == null)
            return null;
        return impl.getHttpMethod();
    }

    public int getStatusCode() {
        if (impl == null)
            return -1;
        return impl.getStatusCode();
    }

    public boolean isError() {
        if (impl == null)
            return true;
        return impl.isError();
    }

    public boolean isSuccess() {
        if (impl == null)
            return false;
        return impl.isSuccess();
    }
    
    public void close() {
        if (impl != null) {
            impl.close();
        }
    }
}
