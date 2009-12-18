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
package com.basho.riak.client.raw;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakConfig;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.request.RiakWalkSpec;
import com.basho.riak.client.response.BucketResponse;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.RiakResponseException;
import com.basho.riak.client.response.StreamHandler;
import com.basho.riak.client.util.ClientHelper;
import com.basho.riak.client.util.ClientUtils;
import com.basho.riak.client.util.Constants;

/**
 * Implementation of the {@link RiakClient} that connects to the Riak Raw
 * interface.
 */
public class RawClient implements RiakClient {

    private ClientHelper helper;
    private String riakBasePath;

    public RawClient(RiakConfig config) {
        helper = new ClientHelper(config);
        riakBasePath = ClientUtils.getPathFromUrl(config.getUrl());
    }

    public RawClient(String url) {
        helper = new ClientHelper(new RiakConfig(url));
        riakBasePath = ClientUtils.getPathFromUrl(url);
    }

    public HttpResponse setBucketSchema(String bucket, JSONObject schema, RequestMeta meta) {
        if (schema != null) {
            try {
                schema = new JSONObject().put(Constants.RAW_FL_SCHEMA, schema);
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
            return new RawBucketResponse(r);
        } catch (JSONException e) {
            throw new RiakResponseException(r, e);
        }
    }

    public BucketResponse listBucket(String bucket) {
        return listBucket(bucket, null);
    }

    // Sends the object's link, user-defined metadata and vclock as HTTP headers
    // and the value as the body
    public RawStoreResponse store(RiakObject object, RequestMeta meta) {
        if (meta == null) {
            meta = new RequestMeta();
        }
        Collection<RiakLink> links = object.getLinks();
        Map<String, String> usermeta = object.getUsermeta();
        StringBuilder linkHeader = new StringBuilder();
        String vclock = object.getVclock();

        if (links != null) {
            for (RiakLink link : links) {
                linkHeader.append("<");
                linkHeader.append(riakBasePath);
                linkHeader.append("/");
                linkHeader.append(link.getBucket());
                linkHeader.append("/");
                linkHeader.append(link.getKey());
                linkHeader.append(">; ");
                linkHeader.append(Constants.RAW_LINK_TAG);
                linkHeader.append("=\"");
                linkHeader.append(link.getTag());
                linkHeader.append("\"");
                linkHeader.append(",");
            }
        }
        if (linkHeader.length() > 0) {
            meta.putHeader(Constants.HDR_LINK, linkHeader.toString());
        }
        if (usermeta != null) {
            for (String name : usermeta.keySet()) {
                meta.putHeader(Constants.HDR_USERMETA_PREFIX + name, usermeta.get(name));
            }
        }
        if (vclock != null) {
            meta.putHeader(Constants.HDR_VCLOCK, vclock);
        }

        return new RawStoreResponse(helper.store(object, meta));
    }

    public RawStoreResponse store(RiakObject object) {
        return store(object, null);
    }

    public RawFetchResponse fetchMeta(String bucket, String key, RequestMeta meta) {
        return new RawFetchResponse(helper.fetchMeta(bucket, key, meta));
    }

    public RawFetchResponse fetchMeta(String bucket, String key) {
        return fetchMeta(bucket, key, null);
    }

    // Fetches the object or all sibling objects if there are multiple
    public RawFetchResponse fetch(String bucket, String key, RequestMeta meta) {
        if (meta == null) {
            meta = new RequestMeta();
        }

        String accept = meta.getQueryParam(Constants.HDR_ACCEPT);
        if (accept == null) {
            meta.putHeader(Constants.HDR_ACCEPT, Constants.CTYPE_ANY + ", " + Constants.CTYPE_MULTIPART_MIXED);
        } else {
            meta.putHeader(Constants.HDR_ACCEPT, accept + ", " + Constants.CTYPE_MULTIPART_MIXED);
        }
        return new RawFetchResponse(helper.fetch(bucket, key, meta));
    }

    public RawFetchResponse fetch(String bucket, String key) {
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

    public RawWalkResponse walk(String bucket, String key, String walkSpec, RequestMeta meta) {
        if (meta == null) {
            meta = new RequestMeta();
        }
        meta.putHeader(Constants.HDR_ACCEPT, Constants.CTYPE_MULTIPART_MIXED);
        return new RawWalkResponse(helper.walk(bucket, key, walkSpec, meta));
    }

    public RawWalkResponse walk(String bucket, String key, String walkSpec) {
        return walk(bucket, key, walkSpec, null);
    }

    public RawWalkResponse walk(String bucket, String key, RiakWalkSpec walkSpec) {
        return walk(bucket, key, walkSpec.toString(), null);
    }
}
