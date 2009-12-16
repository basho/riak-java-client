package com.basho.riak.client.raw;

import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;

import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.StoreResponse;
import com.basho.riak.client.util.Constants;

public class RawStoreResponse implements HttpResponse, StoreResponse {

    private HttpResponse impl;

    public String getVclock() { return vclock; }
    private String vclock = null;
    
    public String getLastmod() { return lastmod; }
    private String lastmod = null;

    public String getVtag() { return vtag; }
    private String vtag = null;

    public RawStoreResponse(HttpResponse r) {

        this.impl = r;
        
        if (r.isSuccess()) {
            Map<String, String> headers = r.getHttpHeaders();
            this.vclock = headers.get(Constants.HDR_VCLOCK);
            this.lastmod = headers.get(Constants.HDR_LAST_MODIFIED);
            this.vtag = headers.get(Constants.HDR_ETAG);
        }
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
