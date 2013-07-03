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
package com.basho.riak.client.http;

import com.basho.riak.client.http.request.IndexRequest;
import com.basho.riak.client.http.request.MapReduceBuilder;
import com.basho.riak.client.http.request.RequestMeta;
import com.basho.riak.client.http.request.RiakWalkSpec;
import com.basho.riak.client.http.response.*;
import com.basho.riak.client.http.util.ClientHelper;
import com.basho.riak.client.http.util.Constants;
import org.apache.http.client.HttpClient;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Primary interface for interacting with Riak via HTTP.
 */
public class RiakClient {

    private ClientHelper helper;

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#getConfig()
     */
    public RiakConfig getConfig() {
        return helper.getConfig();
    }

    public RiakClient(RiakConfig config) {
        this(config, null);
    }

    public RiakClient(RiakConfig config, String clientId) {
        helper = new ClientHelper(config, clientId);
    }

    public RiakClient(String url) {
        this(new RiakConfig(url), null);
    }

    public RiakClient(String url, String clientId) {
        this(new RiakConfig(url), clientId);
    }

    // Package protected constructor used for testing
    RiakClient(ClientHelper helper) {
        this.helper = helper;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#setBucketSchema(java.lang.String, com.basho.riak.client.RiakBucketInfo, com.basho.riak.client.request.RequestMeta)
     */
    public HttpResponse setBucketSchema(String bucket, RiakBucketInfo bucketInfo, RequestMeta meta) {
        JSONObject schema = null;
        try {
            schema = new JSONObject().put(Constants.FL_SCHEMA, bucketInfo.getSchema());
        } catch (JSONException unreached) {
            throw new IllegalStateException("wrapping valid json should be valid", unreached);
        }

        return helper.setBucketSchema(bucket, schema, meta);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#setBucketSchema(java.lang.String, com.basho.riak.client.RiakBucketInfo)
     */
    public HttpResponse setBucketSchema(String bucket, RiakBucketInfo bucketInfo) {
        return setBucketSchema(bucket, bucketInfo, null);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#getBucketSchema(java.lang.String, com.basho.riak.client.request.RequestMeta)
     */
    public BucketResponse getBucketSchema(String bucket, RequestMeta meta) {
        HttpResponse r = helper.getBucketSchema(bucket, meta);
        try {
            return getBucketResponse(r);
        } catch (JSONException e) {
            try {
                return new BucketResponse(helper.toss(new RiakResponseRuntimeException(r, e)));
            } catch (Exception e1) {
                throw new IllegalStateException(
                                                "helper.toss() returns a unsuccessful result, so BucketResponse shouldn't try to parse it or throw");
            }
        } catch (IOException e) {
            try {
                return new BucketResponse(helper.toss(new RiakIORuntimeException(e)));
            } catch (Exception e1) {
                throw new IllegalStateException(
                                                "helper.toss() returns a unsuccessful result, so BucketResponse shouldn't try to read it or throw");
            }
        }
    }

    public HttpResponse resetBucketSchema(String bucket) {
        return helper.resetBucketSchema(bucket);
    }
    
    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#getBucketSchema(java.lang.String)
     */
    public BucketResponse getBucketSchema(String bucket) {
        return getBucketSchema(bucket, null);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#listBucket(java.lang.String, com.basho.riak.client.request.RequestMeta)
     */
    public BucketResponse listBucket(String bucket, RequestMeta meta) {
        return listBucket(bucket, meta, false);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#listBucket(java.lang.String)
     */
    public BucketResponse listBucket(String bucket) {
        return listBucket(bucket, null);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#streamBucket(java.lang.String, com.basho.riak.client.request.RequestMeta)
     */
    public BucketResponse streamBucket(String bucket, RequestMeta meta) {
        return listBucket(bucket, meta, true);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#streamBucket(java.lang.String)
     */
    public BucketResponse streamBucket(String bucket) {
        return streamBucket(bucket, null);
    }

    BucketResponse listBucket(String bucket, RequestMeta meta, boolean streamResponse) {
        HttpResponse r = helper.listBucket(bucket, meta, streamResponse);
        try {
            return getBucketResponse(r);
        } catch (JSONException e) {
            try {
                return new BucketResponse(helper.toss(new RiakResponseRuntimeException(r, e)));
            } catch (Exception e1) {
                throw new IllegalStateException(
                                                "helper.toss() returns a unsuccessful result, so BucketResponse shouldn't try to parse it or throw");
            }
        } catch (IOException e) {
            try {
                return new BucketResponse(helper.toss(new RiakIORuntimeException(e)));
            } catch (Exception e1) {
                throw new IllegalStateException(
                                                "helper.toss() returns a unsuccessful result, so BucketResponse shouldn't try to read it or throw");
            }
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#store(com.basho.riak.client.RiakObject, com.basho.riak.client.request.RequestMeta)
     */
    public StoreResponse store(RiakObject object, RequestMeta meta) {
        if (meta == null) {
            meta = new RequestMeta();
        }
        if (meta.getQueryParam(Constants.QP_RETURN_BODY) == null) {
            meta.setQueryParam(Constants.QP_RETURN_BODY, "true");
        }

        setAcceptHeader(meta);
        HttpResponse r = helper.store(object, meta);
        return new StoreResponse(new FetchResponse(r, this));
    }

    /**
     * Ensure that Accept header includes
     * {@link Constants#CTYPE_MULTIPART_MIXED} so that we may parse siblings
     * correctly.
     * 
     * @param meta
     */
    private void setAcceptHeader(RequestMeta meta) {
        String accept = meta.getHeader(Constants.HDR_ACCEPT);
        if (accept == null) {
            meta.setHeader(Constants.HDR_ACCEPT, Constants.CTYPE_ANY + ", " + Constants.CTYPE_MULTIPART_MIXED);
        } else {
            meta.setHeader(Constants.HDR_ACCEPT, accept + ", " + Constants.CTYPE_MULTIPART_MIXED);
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#store(com.basho.riak.client.RiakObject)
     */
    public StoreResponse store(RiakObject object) {
        return store(object, null);
    }

    /**
     * Fetch metadata (e.g. vclock, last modified, vtag) for the
     * {@link RiakObject} stored at <code>bucket</code> and <code>key</code>.
     * <p>
     * NOTE: if there a sibling values (HTTP status code 300), then a full
     * {@link RiakClient#fetch(String, String, RequestMeta)} is executed as well
     * to get all meta values. Examine the {@link FetchResponse#hasSiblings()}
     * value to determine if you need to perform conflict resolution.
     * </p>
     * 
     * @param bucket
     *            The bucket containing the {@link RiakObject} to fetch.
     * @param key
     *            The key of the {@link RiakObject} to fetch.
     * @param meta
     *            Extra metadata to attach to the request such as an r- value
     *            for the request, HTTP headers, and other query parameters. See
     *            {@link RequestMeta#readParams(int)}.
     * 
     * @return {@link FetchResponse} containing HTTP response information and a
     *         {@link RiakObject} containing only metadata and no value.
     * 
     * @throws RiakIORuntimeException
     *             If an error occurs during communication with the Riak server.
     * @throws RiakResponseRuntimeException
     *             If the Riak server returns a malformed response.
     */
    public FetchResponse fetchMeta(String bucket, String key, RequestMeta meta) {
        try {
            if (meta == null) {
                meta = new RequestMeta();
            }
            setAcceptHeader(meta);
            HttpResponse resp = helper.fetchMeta(bucket, key, meta);

            if (resp.getStatusCode() != 300) {
                return getFetchResponse(resp);
            } else {
                return fetch(bucket, key, meta);
            }
        } catch (RiakResponseRuntimeException e) {
            return new FetchResponse(helper.toss(e), this);
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#fetchMeta(java.lang.String, java.lang.String)
     */
    public FetchResponse fetchMeta(String bucket, String key) {
        return fetchMeta(bucket, key, null);
    }

    public FetchResponse fetch(String bucket, String key, RequestMeta meta) {
        return fetch(bucket, key, meta, false);
    }

    public FetchResponse fetch(String bucket, String key) {
        return fetch(bucket, key, null, false);
    }

    /**
     * Similar to fetch(), except the HTTP connection is left open for
     * successful responses, and the Riak response is provided as a stream.
     * The user must remember to call {@link FetchResponse#close()} on the
     * return value.
     * 
     * @param bucket
     *            The bucket containing the {@link RiakObject} to fetch.
     * @param key
     *            The key of the {@link RiakObject} to fetch.
     * @param meta
     *            Extra metadata to attach to the request such as an r value
     *            for the request, HTTP headers, and other query parameters. See
     *            RequestMeta.readParams().
     * 
     * @return A streaming {@link FetchResponse} containing HTTP response
     *         information and the response stream. The HTTP connection must be
     *         closed manually by the user by calling
     *         {@link FetchResponse#close()}.
     */
    public FetchResponse stream(String bucket, String key, RequestMeta meta) {
        return fetch(bucket, key, meta, true);
    }

    public FetchResponse stream(String bucket, String key) {
        return fetch(bucket, key, null, true);
    }

    FetchResponse fetch(String bucket, String key, RequestMeta meta, boolean streamResponse) {
        if (meta == null) {
            meta = new RequestMeta();
        }

        setAcceptHeader(meta);
        HttpResponse r = helper.fetch(bucket, key, meta, streamResponse);

        try {
            return getFetchResponse(r);
        } catch (RiakResponseRuntimeException e) {
            return new FetchResponse(helper.toss(e), this);
        }

    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#stream(java.lang.String, java.lang.String, com.basho.riak.client.response.StreamHandler, com.basho.riak.client.request.RequestMeta)
     */
    public boolean stream(String bucket, String key, StreamHandler handler, RequestMeta meta) throws IOException {
        return helper.stream(bucket, key, handler, meta);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#delete(java.lang.String, java.lang.String, com.basho.riak.client.request.RequestMeta)
     */
    public HttpResponse delete(String bucket, String key, RequestMeta meta) {
        return helper.delete(bucket, key, meta);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#delete(java.lang.String, java.lang.String)
     */
    public HttpResponse delete(String bucket, String key) {
        return delete(bucket, key, null);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#walk(java.lang.String, java.lang.String, java.lang.String, com.basho.riak.client.request.RequestMeta)
     */
    public WalkResponse walk(String bucket, String key, String walkSpec, RequestMeta meta) {
        HttpResponse r = helper.walk(bucket, key, walkSpec, meta);

        try {
            return getWalkResponse(r);
        } catch (RiakResponseRuntimeException e) {
            return new WalkResponse(helper.toss(e), this);
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#walk(java.lang.String, java.lang.String, java.lang.String)
     */
    public WalkResponse walk(String bucket, String key, String walkSpec) {
        return walk(bucket, key, walkSpec, null);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#walk(java.lang.String, java.lang.String, com.basho.riak.client.request.RiakWalkSpec)
     */
    public WalkResponse walk(String bucket, String key, RiakWalkSpec walkSpec) {
        return walk(bucket, key, walkSpec.toString(), null);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#mapReduce(java.lang.String, com.basho.riak.client.request.RequestMeta)
     */
    public MapReduceResponse mapReduce(String job, RequestMeta meta) {
        HttpResponse r = helper.mapReduce(job, meta);
        try {
            return getMapReduceResponse(r);
        } catch (JSONException e) {
            helper.toss(new RiakResponseRuntimeException(r, e));
            return null;
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#mapReduce(java.lang.String)
     */
    public MapReduceResponse mapReduce(String job) {
        return mapReduce(job, null);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#mapReduceOverBucket(java.lang.String)
     */
    public MapReduceBuilder mapReduceOverBucket(String bucket) {
        return new MapReduceBuilder(this).setBucket(bucket);
    }

    /**
     * Same as {@link RiakClient#mapReduceOverBucket(String)}, except over a set
     * of objects instead of a bucket.
     * 
     * @param objects
     *            A set of objects represented as a map of { bucket : [ list of
     *            keys in bucket ] }
     * 
     * @return A {@link MapReduceBuilder} to build the map reduce job
     */
    public MapReduceBuilder mapReduceOverObjects(Map<String, Set<String>> objects) {
        return new MapReduceBuilder(this).setRiakObjects(objects);
    }

    /**
     * Same as {@link RiakClient#mapReduceOverBucket(String)}, except over a
     * riak-search query instead of a bucket.
     * 
     * @param bucket
     *            The bucket to perform the riak-search over
     * @param search
     *            The query that riak-search will execute
     * @return A {@link MapReduceBuilder} to build the map reduce job
     */
    public MapReduceBuilder mapReduceOverSearch(String bucket, String search) {
        return new MapReduceBuilder(this).setBucket(bucket).setSearch(search);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#getExceptionHandler()
     */
    public RiakExceptionHandler getExceptionHandler() {
        return helper.getExceptionHandler();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#setExceptionHandler(com.basho.riak.client.response.RiakExceptionHandler)
     */
    public void setExceptionHandler(RiakExceptionHandler exceptionHandler) {
        helper.setExceptionHandler(exceptionHandler);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#getHttpClient()
     */
    public HttpClient getHttpClient() {
        return helper.getHttpClient();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#getClientId()
     */
    public byte[] getClientId() {
        return helper.getClientId();
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.HttpRiakClient#setClientId(java.lang.String)
     */
    public void setClientId(String clientId) {
        helper.setClientId(clientId);
    }

    // Encapsulate response creation so it can be stubbed for testing
    BucketResponse getBucketResponse(HttpResponse r) throws JSONException, IOException {
        return new BucketResponse(r);
    }

    FetchResponse getFetchResponse(HttpResponse r) throws RiakResponseRuntimeException {
        return new FetchResponse(r, this);
    }

    WalkResponse getWalkResponse(HttpResponse r) throws RiakResponseRuntimeException {
        return new WalkResponse(r, this);
    }

    MapReduceResponse getMapReduceResponse(HttpResponse r) throws JSONException {
        return new MapReduceResponse(r);
    }

    /**
     * Fetch a list of all the buckets in Riak
     * 
     * @return a {@link ListBucketsResponse}
     * @throws IOException
     * @throws JSONException
     */
    public ListBucketsResponse listBuckets(boolean streaming) {
        HttpResponse r = helper.listBuckets(streaming);
        try {
            return new ListBucketsResponse(r);
        } catch (JSONException e) {
            try {
                return new ListBucketsResponse(helper.toss(new RiakResponseRuntimeException(r, e)));
            } catch (Exception e1) {
                throw new IllegalStateException(
                                                "helper.toss() returns a unsuccessful result, so BucketResponse shouldn't try to parse it or throw");
            }
        } catch (IOException e) {
            try {
                return new ListBucketsResponse(helper.toss(new RiakIORuntimeException(e)));
            } catch (Exception e1) {
                throw new IllegalStateException(
                                                "helper.toss() returns a unsuccessful result, so BucketResponse shouldn't try to read it or throw");
            }
        }
    }

    /**
     * GET Riak's ping resource.
     * 
     * @return an {@link HttpResponse} with the result of GET
     */
    public HttpResponse ping() {
        return helper.ping();
    }

    /**
     * GET Riak's <code>/stats</code> (status) resource.
     * 
     * @return an {@link HttpResponse} with the result of GET
     */
    public HttpResponse stats() {
        return helper.stats();
    }
    
    
    /**
     * Fetch the keys for <code>index</code> with <code>value</code>
     * 
     * @param bucket
     *            the bucket
     * @param indexName
     *            the name of the index (e.g. 'user_bin')
     * @param value
     *            the value of the index
     * @return an {@link IndexResponse}
     */
    public IndexResponse index(String bucket, String indexName, String value) {
        return makeIndexResponse(helper.fetchIndex(bucket, indexName, new String[] { value }));
    }

    /**
     * Fetch the keys for <code>index</code> with <code>value</code>
     * 
     * @param bucket
     *            the bucket
     * @param indexName
     *            index name (e.g. 'age_int')
     * @param value
     *            an int for the index value
     * @return {@link IndexResponse}
     */
    public IndexResponse index(String bucket, String indexName, long value) {
        return makeIndexResponse(helper.fetchIndex(bucket, indexName, new long[] { value }));
    }

    /**
     * A range index query matching a binary index from <code>start</code> to
     * <code>end</code>
     * 
     * @param bucket
     *            the bucket
     * @param indexName
     *            the index (e.g. 'username_bin')
     * @param start
     *            the start value in a range (e.g 'a')
     * @param end
     *            the end value in a range (e.g. 'z')
     * @return an {@link IndexResponse}
     */
    public IndexResponse index(String bucket, String indexName, String start, String end) {
        return makeIndexResponse(helper.fetchIndex(bucket, indexName, new String[] { start, end }));
    }

    /**
     * A range index query matching a int index from <code>start</code> to
     * <code>end</code>
     * 
     * @param bucket
     *            the bucket
     * @param indexName
     *            the index (e.g. 'age_int')
     * @param start
     *            the start value in a range (e.g 16)
     * @param end
     *            the end value in a range (e.g. 32)
     * @return an {@link IndexResponse}
     */
    public IndexResponse index(String bucket, String indexName, long start, long end) {
        return makeIndexResponse(helper.fetchIndex(bucket, indexName, new long[] { start, end }));

    }

    /**
     * Create an {@link IndexResponse} from the given {@link HttpResponse}
     * 
     * @param r
     *            an {@link HttpResponse} from an index query
     * @return an {@link IndexResponse}
     */
    private IndexResponse makeIndexResponse(HttpResponse r) {
        try {
            return new IndexResponse(r);
        } catch (JSONException e) {
            try {
                return new IndexResponse(helper.toss(new RiakResponseRuntimeException(r, e)));
            } catch (Exception e1) {
                throw new IllegalStateException(
                                                "helper.toss() returns a unsuccessful result, so BucketResponse shouldn't try to parse it or throw");
            }
        }
    }

    public IndexResponseV2 index(IndexRequest request) {
        HttpResponse r = helper.fetchIndex(request);
        try {
            return new IndexResponseV2(request, r);
        } catch (JSONException e) {
            try {
                return new IndexResponseV2(request, helper.toss(new RiakResponseRuntimeException(r, e)));
            } catch (Exception e1) {
                throw new IllegalStateException("helper.toss() returns a unsuccessful result, so IndexResponseV2 shouldn't try to parse it or throw");
            }
        }
    }
    
    
    public void shutdown()
    {
    }
}
