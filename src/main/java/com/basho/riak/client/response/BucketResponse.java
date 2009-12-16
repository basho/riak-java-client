package com.basho.riak.client.response;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.object.BucketInfo;
import com.basho.riak.client.util.Constants;

public class BucketResponse implements HttpResponse {

    private HttpResponse impl;

    public BucketInfo getBucketInfo() { return this.bucketInfo; }
    private BucketInfo bucketInfo;

    @SuppressWarnings("unchecked")
    public BucketResponse(HttpResponse r) {
        JSONObject json = null;
        try {
            json = new JSONObject(r.getBody());
        } catch (JSONException e) { 
            json = new JSONObject();
        }

        Map<String, String> schema = new HashMap<String, String>();
        Collection<String> keys = new ArrayList<String>();
        JSONObject jsonSchema = json.optJSONObject(Constants.JIAK_SCHEMA);
        JSONArray jsonKeys = json.optJSONArray(Constants.JIAK_KEYS);
        
        if (jsonSchema != null) {
            for (Iterator iter = jsonSchema.keys(); iter.hasNext(); ) {
                Object obj = iter.next();
                if (obj != null) {
                    String key = obj.toString(); 
                    schema.put(key, jsonSchema.optString(key));
                }
            }
        }
        if (jsonKeys != null) {
            for (int i = 0; i < jsonKeys.length(); i++) {
                keys.add(jsonKeys.optString(i));
            }
        }
        
        this.bucketInfo = new BucketInfo(schema, keys);
        this.impl = r;
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
