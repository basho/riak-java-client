package com.basho.riak.client.jiak;

import org.json.JSONException;

import com.basho.riak.client.BasicClient;
import com.basho.riak.client.RiakConfig;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.request.RiakWalkSpec;
import com.basho.riak.client.util.Constants;

public class JiakClient extends BasicClient {
    
    public JiakClient(RiakConfig config) {
        super(config);
    }

    public JiakClient(String url) { 
        super(new RiakConfig(url));
    }

    @Override
    public JiakStoreResponse store(RiakObject object, RequestMeta meta) {
        if (meta == null) meta = new RequestMeta();
        meta.put(Constants.HDR_CONTENT_TYPE, Constants.CTYPE_JSON);
        meta.put(Constants.HDR_ACCEPT, Constants.CTYPE_JSON);
        meta.addQueryParam(Constants.QP_RETURN_BODY, "true");
        try {
            return new JiakStoreResponse(super.store(object, meta));
        } catch (JSONException e) {
            throw new RiakException(e);
        }
    }
    @Override
    public JiakStoreResponse store(RiakObject object) {
        return store(object, null);
    }

    @Override
    public JiakFetchResponse fetchMeta(String bucket, String key, RequestMeta meta) {
        // Jiak doesn't support HEAD, so just fetch()
        return fetch(bucket, key, meta); 
    }
    @Override
    public JiakFetchResponse fetchMeta(String bucket, String key) {
        return fetchMeta(bucket, key, null);
    }

    @Override
    public JiakFetchResponse fetch(String bucket, String key, RequestMeta meta) {
        if (meta == null) meta = new RequestMeta();
        meta.put(Constants.HDR_ACCEPT, Constants.CTYPE_JSON);
        try {
            return new JiakFetchResponse(super.fetch(bucket, key, meta));
        } catch (JSONException e) {
            throw new RiakException(e);
        }
    }
    @Override
    public JiakFetchResponse fetch(String bucket, String key) {
        return fetch(bucket, key, null);
    }

    @Override
    public JiakWalkResponse walk(String bucket, String key, String walkSpec, RequestMeta meta) {
        if (meta == null) meta = new RequestMeta();
        meta.put(Constants.HDR_ACCEPT, Constants.CTYPE_JSON);
        try {
            return new JiakWalkResponse(super.walk(bucket, key, walkSpec, meta));
        } catch (JSONException e) {
            throw new RiakException(e);
        }
    }
    @Override
    public JiakWalkResponse walk(String bucket, String key, String walkSpec) {
        return walk(bucket, key, walkSpec, null);
    }
    @Override
    public JiakWalkResponse walk(String bucket, String key, RiakWalkSpec walkSpec) {
        return walk(bucket, key, walkSpec.toString(), null);
    }
}
