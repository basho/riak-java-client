package com.basho.riak.client.plain;

import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;

import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.RiakResponseException;

/**
 * A checked decorator for RiakResponseException
 */
public class RiakPlainResponseException extends Exception implements HttpResponse {

    private static final long serialVersionUID = 5932513075276473483L;
    private RiakResponseException impl;

    public RiakPlainResponseException(RiakResponseException e) {
        super(e.getMessage(), e.getCause());
        impl = e;
    }

    public String getBody() {
        return impl.getBody();
    }

    public String getBucket() {
        return impl.getBucket();
    }

    public Map<String, String> getHttpHeaders() {
        return impl.getHttpHeaders();
    }

    public HttpMethod getHttpMethod() {
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

}
