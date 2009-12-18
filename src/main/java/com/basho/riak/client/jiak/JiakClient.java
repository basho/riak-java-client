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

import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakConfig;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.request.RiakWalkSpec;
import com.basho.riak.client.response.BucketResponse;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.RiakResponseException;
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

    public HttpResponse setBucketSchema(String bucket, JSONObject schema, RequestMeta meta) {
        if (schema != null) {
            try {
                schema = new JSONObject().put(Constants.JIAK_FL_SCHEMA, schema);
            } catch (JSONException unreached) {
                throw new IllegalStateException("wrapping valid json should be valid", unreached);
            }
        }
        return helper.setBucketSchema(bucket, schema, meta);
    }

    public HttpResponse setBucketSchema(String bucket, JSONObject schema) {
        return setBucketSchema(bucket, schema, null);
    }

    public BucketResponse listBucket(String bucket, RequestMeta meta) {
        HttpResponse r = helper.listBucket(bucket, meta);
        try {
            return new JiakBucketResponse(r);
        } catch (JSONException e) {
            throw new RiakResponseException(r, e);
        }
    }

    public BucketResponse listBucket(String bucket) {
        return listBucket(bucket, null);
    }

    // Jiak stores the object value and metadata all in the message body. Ask
    // Jiak to return the object that was just stored so we can get the updated
    // metadata.
    public JiakStoreResponse store(RiakObject object, RequestMeta meta) {
        if (meta == null) {
            meta = new RequestMeta();
        }
        meta.putHeader(Constants.HDR_CONTENT_TYPE, Constants.CTYPE_JSON);
        meta.putHeader(Constants.HDR_ACCEPT, Constants.CTYPE_JSON);
        meta.addQueryParam(Constants.QP_RETURN_BODY, "true");
        HttpResponse r = helper.store(object, meta);
        try {
            return new JiakStoreResponse(r);
        } catch (JSONException e) {
            throw new RiakResponseException(r, e);
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
        if (meta == null) {
            meta = new RequestMeta();
        }
        meta.putHeader(Constants.HDR_ACCEPT, Constants.CTYPE_JSON);
        HttpResponse r = helper.fetch(bucket, key, meta);
        try {
            return new JiakFetchResponse(r);
        } catch (JSONException e) {
            throw new RiakResponseException(r, e);
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
        if (meta == null) {
            meta = new RequestMeta();
        }
        meta.putHeader(Constants.HDR_ACCEPT, Constants.CTYPE_JSON);
        HttpResponse r = helper.walk(bucket, key, walkSpec, meta);
        try {
            return new JiakWalkResponse(r);
        } catch (JSONException e) {
            throw new RiakResponseException(r, e);
        }
    }

    public JiakWalkResponse walk(String bucket, String key, String walkSpec) {
        return walk(bucket, key, walkSpec, null);
    }

    public JiakWalkResponse walk(String bucket, String key, RiakWalkSpec walkSpec) {
        return walk(bucket, key, walkSpec.toString(), null);
    }
}
