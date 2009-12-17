package com.basho.riak.client.raw;

import java.util.Collection;
import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.RiakBucketInfo;
import com.basho.riak.client.response.BucketResponse;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.util.ClientUtils;
import com.basho.riak.client.util.Constants;

public class RawBucketResponse implements BucketResponse {

    private HttpResponse impl;
    private RiakBucketInfo bucketInfo;

    public RawBucketResponse(HttpResponse r) throws JSONException {
        impl = r;

        if (r.isSuccess()) {
            JSONObject json = new JSONObject(r.getBody());
            JSONObject jsonProps = json.optJSONObject(Constants.RAW_FL_PROPS);
            JSONArray jsonKeys = json.optJSONArray(Constants.RAW_FL_KEYS);

            Map<String, String> props = ClientUtils.jsonObjectAsMap(jsonProps);
            Collection<String> keys = ClientUtils.jsonArrayAsList(jsonKeys);

            bucketInfo = new RiakBucketInfo(props, keys);
        }
    }

    public RiakBucketInfo getBucketInfo() {
        return bucketInfo;
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
