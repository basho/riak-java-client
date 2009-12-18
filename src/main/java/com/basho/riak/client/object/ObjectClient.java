package com.basho.riak.client.object;

import java.io.IOException;
import java.util.List;

import org.json.JSONObject;

import com.basho.riak.client.RiakBucketInfo;
import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakConfig;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.jiak.JiakClient;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.request.RiakWalkSpec;
import com.basho.riak.client.response.BucketResponse;
import com.basho.riak.client.response.FetchResponse;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.RiakIOException;
import com.basho.riak.client.response.RiakResponseException;
import com.basho.riak.client.response.StoreResponse;
import com.basho.riak.client.response.StreamHandler;
import com.basho.riak.client.response.WalkResponse;

public class ObjectClient {

    private RiakClient impl;

    public static ObjectClient connectToJiak(RiakConfig config) {
        return new ObjectClient(new JiakClient(config));
    }

    public static ObjectClient connectToJiak(String url) {
        return new ObjectClient(new JiakClient(url));
    }

    public static ObjectClient connectToRaw(RiakConfig config) {
        return new ObjectClient(new RawClient(config));
    }

    public static ObjectClient connectToRaw(String url) {
        return new ObjectClient(new RawClient(url));
    }

    public ObjectClient(RiakClient riakClient) {
        impl = riakClient;
    }

    public void setBucketSchema(String bucket, JSONObject schema, RequestMeta meta) throws RiakIOException,
            RiakResponseException {
        HttpResponse r = impl.setBucketSchema(bucket, schema, meta);
        if (r.getStatusCode() != 204)
            throw new RiakResponseException(r, r.getBody());
    }

    public void setBucketSchema(String bucket, JSONObject schema) throws RiakIOException, RiakResponseException {
        setBucketSchema(bucket, schema, null);
    }

    public RiakBucketInfo listBucket(String bucket, RequestMeta meta) throws RiakIOException, RiakResponseException {
        BucketResponse r = impl.listBucket(bucket, meta);

        if (r.getStatusCode() != 200)
            throw new RiakResponseException(r, r.getBody());

        return r.getBucketInfo();
    }

    public RiakBucketInfo listBucket(String bucket) throws RiakIOException, RiakResponseException {
        return listBucket(bucket, null);
    }

    public void store(RiakObject object, RequestMeta meta) throws RiakIOException, RiakResponseException {
        StoreResponse r = impl.store(object, meta);

        if (r.getStatusCode() != 200 && r.getStatusCode() != 204)
            throw new RiakResponseException(r, r.getBody());

        object.updateMeta(r.getVclock(), r.getLastmod(), r.getVtag());
    }

    public void store(RiakObject object) throws RiakIOException, RiakResponseException {
        store(object, null);
    }

    public RiakObject fetchMeta(String bucket, String key, RequestMeta meta) throws RiakIOException,
            RiakResponseException {
        FetchResponse r = impl.fetchMeta(bucket, key, meta);

        if (r.getStatusCode() == 404)
            return null;

        if (r.getStatusCode() != 200 && r.getStatusCode() != 304)
            throw new RiakResponseException(r, r.getBody());

        if (!r.hasObject())
            throw new RiakResponseException(r, "Failed to parse metadata");

        return r.getObject();
    }

    public RiakObject fetchMeta(String bucket, String key) throws RiakIOException, RiakResponseException {
        return fetchMeta(bucket, key, null);
    }

    public RiakObject fetch(String bucket, String key, RequestMeta meta) throws RiakIOException, RiakResponseException {
        FetchResponse r = impl.fetch(bucket, key, meta);

        if (r.getStatusCode() == 404)
            return null;

        if (r.getStatusCode() != 200 && r.getStatusCode() != 304)
            throw new RiakResponseException(r, r.getBody());

        if (!r.hasObject())
            throw new RiakResponseException(r, "Failed to parse object");

        return r.getObject();
    }

    public RiakObject fetch(String bucket, String key) throws RiakIOException, RiakResponseException {
        return fetch(bucket, key, null);
    }

    public boolean stream(String bucket, String key, StreamHandler handler, RequestMeta meta) throws IOException {
        return impl.stream(bucket, key, handler, meta);
    }

    public void delete(String bucket, String key, RequestMeta meta) throws RiakIOException, RiakResponseException {
        HttpResponse r = impl.delete(bucket, key, meta);

        if (r.getStatusCode() != 204 && r.getStatusCode() != 404)
            throw new RiakResponseException(r, r.getBody());
    }

    public void delete(String bucket, String key) throws RiakIOException, RiakResponseException {
        delete(bucket, key, null);
    }

    public List<? extends List<? extends RiakObject>> walk(String bucket, String key, String walkSpec, RequestMeta meta)
            throws RiakIOException, RiakResponseException {
        WalkResponse r = impl.walk(bucket, key, walkSpec, meta);

        if (r.getStatusCode() == 404)
            return null;

        if (r.getStatusCode() != 200)
            throw new RiakResponseException(r, r.getBody());

        if (!r.hasSteps())
            throw new RiakResponseException(r, "Failed to parse walk results");

        return r.getSteps();
    }

    public List<? extends List<? extends RiakObject>> walk(String bucket, String key, String walkSpec)
            throws RiakIOException, RiakResponseException {
        return walk(bucket, key, walkSpec, null);
    }

    public List<? extends List<? extends RiakObject>> walk(String bucket, String key, RiakWalkSpec walkSpec)
            throws RiakIOException, RiakResponseException {
        return walk(bucket, key, walkSpec.toString(), null);
    }
}
