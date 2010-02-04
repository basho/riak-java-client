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
package com.basho.riak.client.jiak;

import java.io.IOException;

import org.apache.commons.httpclient.HttpClient;
import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.RiakBucketInfo;
import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakConfig;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.request.RiakWalkSpec;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.RiakExceptionHandler;
import com.basho.riak.client.response.RiakResponseRuntimeException;
import com.basho.riak.client.response.StreamHandler;
import com.basho.riak.client.util.ClientHelper;
import com.basho.riak.client.util.Constants;

/**
 * Implementation of the {@link RiakClient} that connects to the Jiak interface.
 */
public class JiakClient implements RiakClient {

    private ClientHelper helper;

    public JiakClient(RiakConfig config) {
        helper = new ClientHelper(config);
    }

    public JiakClient(String url) {
        helper = new ClientHelper(new RiakConfig(url));
    }

    // Package protected constructor used for testing
    JiakClient(ClientHelper helper) {
        this.helper = helper;
    }

    public HttpResponse setBucketSchema(String bucket, RiakBucketInfo bucketInfo, RequestMeta meta) {
        JSONObject schema = null;
        try {
            schema = new JSONObject().put(Constants.JIAK_FL_SCHEMA, bucketInfo.getSchema());
        } catch (JSONException unreached) {
            throw new IllegalStateException("wrapping valid json should be valid", unreached);
        }

        return helper.setBucketSchema(bucket, schema, meta);
    }

    public HttpResponse setBucketSchema(String bucket, RiakBucketInfo bucketInfo) {
        return setBucketSchema(bucket, bucketInfo, null);
    }

    public JiakBucketResponse listBucket(String bucket, RequestMeta meta) {
        HttpResponse r = helper.listBucket(bucket, meta);
        try {
            return new JiakBucketResponse(r);
        } catch (JSONException e) {
            try {
                return new JiakBucketResponse(helper.toss(new RiakResponseRuntimeException(r, e)));
            } catch (JSONException unreached) {
                throw new IllegalStateException("JiakBucketResponse doesn't throw on helper.toss() return value",
                                                unreached);
            }
        }
    }

    public JiakBucketResponse listBucket(String bucket) {
        return listBucket(bucket, null);
    }

    public JiakStoreResponse store(RiakObject object, RequestMeta meta) {
        if (meta == null) {
            meta = new RequestMeta();
        }

        // Ask Jiak to return the object that was just stored so we can get the
        // updated metadata.
        if (meta.getQueryParam(Constants.QP_RETURN_BODY) == null) {
            meta.setQueryParam(Constants.QP_RETURN_BODY, "true");
        }
        
        HttpResponse r = helper.store(object, meta);
        try {
            return new JiakStoreResponse(r);
        } catch (JSONException e) {
            try {
                return new JiakStoreResponse(helper.toss(new RiakResponseRuntimeException(r, e)));
            } catch (JSONException unreached) {
                throw new IllegalStateException("JiakStoreResponse doesn't throw on helper.toss() return value",
                                                unreached);
            }
        }
    }

    public JiakStoreResponse store(RiakObject object) {
        return store(object, null);
    }

    public JiakFetchResponse fetchMeta(String bucket, String key, RequestMeta meta) {
        // Jiak doesn't support HEAD, so just fetch()
        return fetch(bucket, key, meta);
    }

    public JiakFetchResponse fetchMeta(String bucket, String key) {
        return fetchMeta(bucket, key, null);
    }

    public JiakFetchResponse fetch(String bucket, String key, RequestMeta meta) {
        HttpResponse r = helper.fetch(bucket, key, meta);
        try {
            return new JiakFetchResponse(r);
        } catch (JSONException e) {
            try {
                return new JiakFetchResponse(helper.toss(new RiakResponseRuntimeException(r, e)));
            } catch (JSONException unreached) {
                throw new IllegalStateException("JiakFetchResponse doesn't throw on helper.toss() return value",
                                                unreached);
            }
        }
    }

    public JiakFetchResponse fetch(String bucket, String key) {
        return fetch(bucket, key, null);
    }

    public boolean stream(String bucket, String key, StreamHandler handler, RequestMeta meta) throws IOException {
        return helper.stream(bucket, key, handler, meta);
    }

    public HttpResponse delete(String bucket, String key, RequestMeta meta) {
        return helper.delete(bucket, key, meta);
    }

    public HttpResponse delete(String bucket, String key) {
        return delete(bucket, key, null);
    }

    public JiakWalkResponse walk(String bucket, String key, String walkSpec, RequestMeta meta) {
        HttpResponse r = helper.walk(bucket, key, walkSpec, meta);
        try {
            return new JiakWalkResponse(r);
        } catch (JSONException e) {
            try {
                return new JiakWalkResponse(helper.toss(new RiakResponseRuntimeException(r, e)));
            } catch (JSONException unreached) {
                throw new IllegalStateException("JiakWalkResponse doesn't throw on helper.toss() return value",
                                                unreached);
            }
        }
    }

    public JiakWalkResponse walk(String bucket, String key, String walkSpec) {
        return walk(bucket, key, walkSpec, null);
    }

    public JiakWalkResponse walk(String bucket, String key, RiakWalkSpec walkSpec) {
        return walk(bucket, key, walkSpec.toString(), null);
    }

    /** The installed exception handler or null if not installed */
    public RiakExceptionHandler getExceptionHandler() {
        return helper.getExceptionHandler();
    }

    /**
     * If an exception handler is provided, then the Riak client will hand
     * exceptions to the handler rather than throwing them.
     */
    public void setExceptionHandler(RiakExceptionHandler exceptionHandler) {
        helper.setExceptionHandler(exceptionHandler);
    }

    /** Return the {@link HttpClient} used to make requests, which can be configured. */
    public HttpClient getHttpClient() {
        return helper.getHttpClient();
    }
}
