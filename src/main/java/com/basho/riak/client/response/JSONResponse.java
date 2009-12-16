package com.basho.riak.client.response;

import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;
import org.json.JSONException;
import org.json.JSONObject;


public class JSONResponse implements HttpResponse {

    private HttpResponse impl;

    public JSONObject getObject() { return object; }
    private JSONObject object = null;

    public JSONResponse(HttpResponse r) {
        this.impl = r;
        try {
            this.object = new JSONObject(r.getBody());
        } catch (JSONException e) { }
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
