package com.basho.riak.client.jiak;

import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;
import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.StoreResponse;
import com.basho.riak.client.util.Constants;

public class JiakStoreResponse implements HttpResponse, StoreResponse {

    private HttpResponse impl;
    private JSONObject object = new JSONObject();

    public JiakStoreResponse(HttpResponse r) throws JSONException {
        this.impl = r;
        this.object = new JSONObject(r.getBody());
    }

    public String getVclock() { return object.optString(Constants.JIAK_VCLOCK); }
    public String getLastmod() { return object.optString(Constants.JIAK_LAST_MODIFIED); }
    public String getVtag() { return object.optString(Constants.JIAK_VTAG); }

    public String getBody() { return impl.getBody(); }
    public String getBucket() { return impl.getBucket(); }
    public Map<String, String> getHttpHeaders() { return impl.getHttpHeaders(); }
    public HttpMethod getHttpMethod() { return impl.getHttpMethod(); } 
    public String getKey() { return impl.getKey(); }
    public int getStatusCode() { return impl.getStatusCode(); }
    public boolean isError() { return impl.isError(); }
    public boolean isSuccess() { return impl.isSuccess(); }

}
