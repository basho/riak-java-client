package com.basho.riak.client.jiak;

import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;
import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.response.FetchResponse;
import com.basho.riak.client.response.HttpResponse;

public class JiakFetchResponse implements HttpResponse, FetchResponse {
    
    private HttpResponse impl;

    public JiakObject getObject() { return this.object; }
    public boolean hasObject() { return this.object != null; }
    private JiakObject object;

    public JiakFetchResponse(HttpResponse r) throws JSONException {
        this.impl = r;
        this.object = new JiakObject(new JSONObject(r.getBody()));
    }

    public String getBody() { return impl.getBody(); }
    public String getBucket() { return impl.getBucket(); }
    public Map<String, String> getHttpHeaders() { return impl.getHttpHeaders(); }
    public HttpMethod getHttpMethod() { return impl.getHttpMethod(); } 
    public String getKey() { return impl.getKey(); }
    public int getStatusCode() { return impl.getStatusCode(); }
    public boolean isError() { return impl.isError(); }
    public boolean isSuccess() { return impl.isSuccess(); }
}
