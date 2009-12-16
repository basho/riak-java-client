package com.basho.riak.client;

import java.io.IOException;
import java.util.List;

import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.request.RiakWalkSpec;
import com.basho.riak.client.response.BasicResponse;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.JSONResponse;
import com.basho.riak.client.response.StreamHandler;

public interface RiakClient {

    public BasicResponse setBucketSchema(String bucket,
            List<String> allowedFields, List<String> writeMask,
            List<String> readMask, List<String> requiredFields,
            RequestMeta meta);
    public BasicResponse setBucketSchema(String bucket,
            List<String> allowedFields, List<String> writeMask,
            List<String> readMask, List<String> requiredFields);

    public JSONResponse listBucket(String bucket, RequestMeta meta);
    public JSONResponse listBucket(String bucket);
            
    public HttpResponse store(RiakObject object, RequestMeta meta);
    public HttpResponse store(RiakObject object);

    public HttpResponse fetchMeta(String bucket, String key, RequestMeta meta);
    public HttpResponse fetchMeta(String bucket, String key);

    public HttpResponse fetch(String bucket, String key, RequestMeta meta);
    public HttpResponse fetch(String bucket, String key);

    public boolean stream(String bucket, String key, StreamHandler handler, RequestMeta meta) throws IOException;

    public HttpResponse delete(String bucket, String key, RequestMeta meta);
    public HttpResponse delete(String bucket, String key);

    public HttpResponse walk(String bucket, String key, String walkSpec);

    public HttpResponse walk(String bucket, String key, RiakWalkSpec walkSpec);

}