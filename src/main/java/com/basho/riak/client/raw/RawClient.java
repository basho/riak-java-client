/*
 * This file is provided to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain
 * a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.basho.riak.client.raw;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.json.JSONException;

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

    public HttpResponse setBucketSchema(String bucket, Map<String, Object> schema, RequestMeta meta) {
        return helper.setBucketSchema(bucket, Constants.RAW_FL_PROPS, schema, meta);
    }

    public HttpResponse setBucketSchema(String bucket, Map<String, Object> schema) {
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
                linkHeader.append("<").append(riakBasePath).append("/").append(link.getBucket()).append("/").append(
                                                                                                                    link.getKey()).append(
                                                                                                                                          ">; ").append(
                                                                                                                                                        Constants.RAW_LINK_TAG).append(
                                                                                                                                                                                       "=\"").append(
                                                                                                                                                                                                     link.getTag()).append(
                                                                                                                                                                                                                           "\"").append(
                                                                                                                                                                                                                                        ",");
            }
        }
        if (linkHeader.length() > 0) {
            meta.put(Constants.HDR_LINK, linkHeader.toString());
        }
        if (usermeta != null) {
            for (String name : usermeta.keySet()) {
                meta.put(Constants.HDR_USERMETA_PREFIX + name, usermeta.get(name));
            }
        }
        if (vclock != null) {
            meta.put(Constants.HDR_VCLOCK, vclock);
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

    public RawFetchResponse fetch(String bucket, String key, RequestMeta meta) {
        if (meta == null) {
            meta = new RequestMeta();
        }

        String accept = meta.getQueryParam(Constants.HDR_ACCEPT);
        if (accept == null) {
            meta.put(Constants.HDR_ACCEPT, Constants.CTYPE_ANY + ", " + Constants.CTYPE_MULTIPART_MIXED);
        } else {
            meta.put(Constants.HDR_ACCEPT, accept + ", " + Constants.CTYPE_MULTIPART_MIXED);
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
        meta.put(Constants.HDR_ACCEPT, Constants.CTYPE_MULTIPART_MIXED);
        return new RawWalkResponse(helper.walk(bucket, key, walkSpec, meta));
    }

    public RawWalkResponse walk(String bucket, String key, String walkSpec) {
        return walk(bucket, key, walkSpec, null);
    }

    public RawWalkResponse walk(String bucket, String key, RiakWalkSpec walkSpec) {
        return walk(bucket, key, walkSpec.toString(), null);
    }
}
