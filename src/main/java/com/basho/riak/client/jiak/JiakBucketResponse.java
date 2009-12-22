package com.basho.riak.client.jiak;

import java.util.Collection;
import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.response.BucketResponse;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.util.ClientUtils;
import com.basho.riak.client.util.Constants;

/**
 * Decorates an HttpResponse to interpret listBucket response from Riak's Jiak
 * interface which returns a JSON object with the keys "schema" and "keys".
 */
public class JiakBucketResponse implements BucketResponse {

    private HttpResponse impl;
    private JiakBucketInfo bucketInfo = null;

    /**
     * On a 2xx response, parses the JSON response into a {@link JiakBucketInfo}
     * .
     * 
     * @param r
     *            The HTTP response from a GET at a Jiak bucket
     * @throws JSONException
     *             If the response is a 2xx but contains invalid JSON
     */
    public JiakBucketResponse(HttpResponse r) throws JSONException {
        impl = r;

        if (r.isSuccess() && (r.getBody() != null)) {
            JSONObject json = new JSONObject(r.getBody());
            JSONObject schema = json.optJSONObject(Constants.JIAK_FL_SCHEMA);
            JSONArray jsonKeys = json.optJSONArray(Constants.JIAK_FL_SCHEMA_KEYS);
            Collection<String> keys = ClientUtils.jsonArrayAsList(jsonKeys);

            bucketInfo = new JiakBucketInfo(schema, keys);
        }
    }

    public boolean hasBucketInfo() {
        return bucketInfo != null;
    }

    public JiakBucketInfo getBucketInfo() {
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
