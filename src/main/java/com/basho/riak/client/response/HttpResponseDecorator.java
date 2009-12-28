package com.basho.riak.client.response;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;

/**
 * A default decorator implementation for HttpResponse
 */
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

    public String getBody() {
        if (impl == null)
            return null;
        return impl.getBody();
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
}
