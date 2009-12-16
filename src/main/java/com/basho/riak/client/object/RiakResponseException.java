package com.basho.riak.client.object;

import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;

import com.basho.riak.client.response.HttpResponse;

public class RiakResponseException extends Exception implements HttpResponse {

    private static final long serialVersionUID = 2853253336513247178L;
    private HttpResponse response = null;
    
    public RiakResponseException(HttpResponse response) {
        super();
        this.response = response;
    }
    
    public RiakResponseException(HttpResponse response, String message, Throwable cause) {
        super(message, cause);
        this.response = response;
    }

    public RiakResponseException(HttpResponse response, String message) {
        super(message);
        this.response = response;
    }

    public RiakResponseException(HttpResponse response, Throwable cause) {
        super(cause);
        this.response = response;
    }

    public String getBody() { 
        if (response == null)
            return null;
        return response.getBody(); 
    }

    public String getBucket() {
        if (response == null)
            return null;
        return response.getBucket(); 
    }

    public Map<String, String> getHttpHeaders() {
        if (response == null)
            return null;
        return response.getHttpHeaders(); 
    }

    public HttpMethod getHttpMethod() {
        if (response == null)
            return null;
        return response.getHttpMethod(); 
    }

    public String getKey() {
        if (response == null)
            return null;
        return response.getKey(); 
    }

    public int getStatusCode() {
        if (response == null)
            return -1;
        return response.getStatusCode(); 
    }

    public boolean isError() {
        return true;
    }

    public boolean isSuccess() {
        return false;
    }

}
