package com.basho.riak.client.raw;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;

import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.WalkResponse;

public class RawWalkResponse implements HttpResponse, WalkResponse {

    private HttpResponse impl;
    
    public List<? extends List<RawObject>> getSteps() { return steps; }
    private List<? extends List<RawObject>> steps;

    public RawWalkResponse(HttpResponse r) {
        this.impl = r;
        this.steps = parseSteps(r.getBody());
    }

    public String getBody() { return impl.getBody(); }
    public String getBucket() { return impl.getBucket(); }
    public Map<String, String> getHttpHeaders() { return impl.getHttpHeaders(); }
    public HttpMethod getHttpMethod() { return impl.getHttpMethod(); } 
    public String getKey() { return impl.getKey(); }
    public int getStatusCode() { return impl.getStatusCode(); }
    public boolean isError() { return impl.isError(); }
    public boolean isSuccess() { return impl.isSuccess(); }

    private static List<? extends List<RawObject>> parseSteps(String body) {
        List<? extends List<RawObject>> steps = new ArrayList<ArrayList<RawObject>>();
        return steps;
    }
}
