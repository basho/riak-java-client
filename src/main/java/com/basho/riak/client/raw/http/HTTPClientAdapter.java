/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.client.raw.http;

import static com.basho.riak.client.raw.http.ConversionUtil.convert;

import java.io.IOException;
import java.util.Iterator;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.bucket.BucketProperties;
import com.basho.riak.client.cap.ClientId;
import com.basho.riak.client.http.RiakClient;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.WalkResult;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.StoreMeta;
import com.basho.riak.client.raw.query.LinkWalkSpec;
import com.basho.riak.client.raw.query.MapReduceSpec;
import com.basho.riak.client.raw.query.MapReduceTimeoutException;
import com.basho.riak.client.http.request.RequestMeta;
import com.basho.riak.client.http.response.BucketResponse;
import com.basho.riak.client.http.response.FetchResponse;
import com.basho.riak.client.http.response.HttpResponse;
import com.basho.riak.client.http.response.MapReduceResponse;
import com.basho.riak.client.http.response.StoreResponse;
import com.basho.riak.client.http.response.WithBodyResponse;

/**
 * Adapts the old {@link RiakClient} to the new {@link RawClient} interface.
 * 
 * @author russell
 * 
 */
public class HTTPClientAdapter implements RawClient {

    private final RiakClient client;

    /**
     * @param client
     */
    public HTTPClientAdapter(RiakClient client) {
        this.client = client;
    }

    /**
     * @param string
     * @param i
     */
    public HTTPClientAdapter(String url) {
        this(new RiakClient(url));
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#fetch(com.basho.riak.newapi.bucket
     * .Bucket, java.lang.String)
     */
    public RiakResponse fetch(String bucket, String key) throws IOException {
        if (bucket == null ||  bucket.trim().equals("")) {
            throw new IllegalArgumentException(
                                               "bucket must not be null and bucket.getName() must not be null or empty "
                                                       + "or just whitespace.");
        }

        if (key == null || key.trim().equals("")) {
            throw new IllegalArgumentException("Key cannot be null or empty or just whitespace");
        }

        FetchResponse resp = client.fetch(bucket, key);

        return handleBodyResponse(resp);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#fetch(com.basho.riak.newapi.bucket
     * .Bucket, java.lang.String, int)
     */
    public RiakResponse fetch(String bucket, String key, int readQuorum) throws IOException {
        if (bucket == null ||  bucket.trim().equals("")) {
            throw new IllegalArgumentException(
                                               "bucket must not be null and bucket.getName() must not be null or empty "
                                                       + "or just whitespace.");
        }

        if (key == null || key.trim().equals("")) {
            throw new IllegalArgumentException("Key cannot be null or empty or just whitespace");
        }

        FetchResponse resp = client.fetch(bucket, key, RequestMeta.readParams(readQuorum));

        return handleBodyResponse(resp);
    }

    /**
     * @param bucket
     * @param resp
     * @return
     */
    private RiakResponse handleBodyResponse(WithBodyResponse resp) {
        RiakResponse response = RiakResponse.empty();
        IRiakObject[] values = new IRiakObject[] {};

        if (resp.hasSiblings()) {
            values = convert(resp.getSiblings());
        } else if (resp.hasObject()) {
            values = new IRiakObject[] { convert(resp.getObject()) };
        }

        if (values.length > 0) {
            response = new RiakResponse(resp.getObject().getVclock().getBytes(), values);
        }

        return response;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#store(com.basho.riak.newapi.RiakObject
     * , com.basho.riak.client.raw.StoreMeta)
     */
    public RiakResponse store(IRiakObject object, StoreMeta storeMeta) throws IOException {
        if (object == null || object.getBucket() == null) {
            throw new IllegalArgumentException("cannot store a null RiakObject, or a RiakObject without a bucket");
        }
        RiakResponse response = RiakResponse.empty();

        com.basho.riak.client.http.RiakObject riakObject = convert(object, client);
        RequestMeta requestMeta = convert(storeMeta);
        StoreResponse resp = client.store(riakObject, requestMeta);

        if (resp.isSuccess()) {
            riakObject.updateMeta(resp);
        } else {
            throw new IOException(resp.getBodyAsString());
        }

        if (storeMeta.hasReturnBody() && storeMeta.getReturnBody()) {
            response = handleBodyResponse(resp);
        }

        return response;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#store(com.basho.riak.newapi.RiakObject
     * )
     */
    public void store(IRiakObject object) throws IOException {
        store(object, new StoreMeta(null, null, false));
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#delete(com.basho.riak.newapi.bucket
     * .Bucket, java.lang.String)
     */
    public void delete(String bucket, String key) throws IOException {
        HttpResponse resp = client.delete(bucket, key);
        if (!resp.isSuccess()) {
            throw new IOException(resp.getBodyAsString());
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#delete(com.basho.riak.newapi.bucket
     * .Bucket, java.lang.String, int)
     */
    public void delete(String bucket, String key, int deleteQuorum) throws IOException {
        HttpResponse resp = client.delete(bucket, key, RequestMeta.deleteParams(deleteQuorum));
        if (!resp.isSuccess()) {
            throw new IOException(resp.getBodyAsString());
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#listBuckets()
     */
    public Iterator<String> listBuckets() throws IOException {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#fetchBucket(java.lang.String)
     */
    public BucketProperties fetchBucket(String bucketName) throws IOException {
        if (bucketName == null || bucketName.trim().equals("")) {
            throw new IllegalArgumentException("bucketName cannot be null, empty or all whitespace");
        }

        BucketResponse response = client.getBucketSchema(bucketName, null);

        return convert(response);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#updateBucket(java.lang.String,
     * com.basho.riak.newapi.bucket.BucketProperties)
     */
    public void updateBucket(String name, BucketProperties bucketProperties) throws IOException {
        HttpResponse response = client.setBucketSchema(name, convert(bucketProperties));
        if (!response.isSuccess()) {
            throw new IOException(response.getBodyAsString());
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#fetchBucketKeys(java.lang.String)
     */
    public Iterable<String> listKeys(String bucketName) throws IOException {
        final BucketResponse bucketResponse = client.streamBucket(bucketName);
        final KeySource keyStream = new KeySource(bucketResponse);
        return new Iterable<String>() {
            public Iterator<String> iterator() {
                return keyStream;
            }
        };
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#linkWalk(com.basho.riak.client.raw.query.LinkWalkSpec)
     */
    public WalkResult linkWalk(final LinkWalkSpec linkWalkSpec) throws IOException {
        final String walkSpecString = convert(linkWalkSpec);
        return convert(client.walk(linkWalkSpec.getStartBucket(), linkWalkSpec.getStartKey(), walkSpecString));
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.RawClient#mapReduce(com.basho.riak.newapi.query
     * .MapReduceSpec)
     */
    public MapReduceResult mapReduce(MapReduceSpec spec) throws IOException, MapReduceTimeoutException {
        MapReduceResponse resp = client.mapReduce(spec.getJSON());
        return convert(resp);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#generateAndSetClientId()
     */
    public byte[] generateAndSetClientId() throws IOException {
        byte[] clientId = ClientId.generate();

        client.setClientId(new String(clientId));
        return client.getClientId();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#setClientId(byte[])
     */
    public void setClientId(byte[] clientId) throws IOException {
        if (clientId == null || clientId.length != 4) {
            throw new IllegalArgumentException("clientId must be 4 bytes. generateAndSetClientId() can do this for you");
        }
        client.setClientId(new String(clientId));
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.raw.RawClient#getClientId()
     */
    public byte[] getClientId() throws IOException {
        return client.getClientId();
    }
}
