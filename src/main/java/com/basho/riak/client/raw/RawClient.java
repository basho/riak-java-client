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

import com.basho.riak.client.RiakBucketInfo;
import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakConfig;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.request.RiakWalkSpec;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.RiakExceptionHandler;
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

    public HttpResponse setBucketSchema(String bucket, RiakBucketInfo bucketInfo, RequestMeta meta) {
        JSONObject schema = null;
        try {
            schema = new JSONObject().put(Constants.RAW_FL_SCHEMA, bucketInfo.getSchema());
        } catch (JSONException unreached) {
            throw new IllegalStateException("wrapping valid json should be valid", unreached);
        }

        return helper.setBucketSchema(bucket, schema, meta);
    }

    public HttpResponse setBucketSchema(String bucket, RiakBucketInfo bucketInfo) {
        return setBucketSchema(bucket, bucketInfo, null);
    }

    public RawBucketResponse listBucket(String bucket, RequestMeta meta) {
        HttpResponse r = helper.listBucket(bucket, meta);
        if (r == null)
            return null;
        try {
            return new RawBucketResponse(r);
        } catch (JSONException e) {
            helper.toss(new RiakResponseException(r, e));
            return null;
        }
    }

    public RawBucketResponse listBucket(String bucket) {
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
                if (linkHeader.length() > 0) {
                    linkHeader.append(", ");
                }
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
            }
        }
        if (linkHeader.length() > 0) {
            meta.setHeader(Constants.HDR_LINK, linkHeader.toString());
        }
        if (usermeta != null) {
            for (String name : usermeta.keySet()) {
                meta.setHeader(Constants.HDR_USERMETA_PREFIX + name, usermeta.get(name));
            }
        }
        if (vclock != null) {
            meta.setHeader(Constants.HDR_VCLOCK, vclock);
        }

        HttpResponse r = helper.store(object, meta);
        if (r == null)
            return null;

        return new RawStoreResponse(r);
    }

    public RawStoreResponse store(RiakObject object) {
        return store(object, null);
    }

    public RawFetchResponse fetchMeta(String bucket, String key, RequestMeta meta) {
        try {
            return new RawFetchResponse(helper.fetchMeta(bucket, key, meta));
        } catch (RiakResponseException e) {
            return new RawFetchResponse(helper.toss(e));
        }
    }

    public RawFetchResponse fetchMeta(String bucket, String key) {
        return fetchMeta(bucket, key, null);
    }

    // Fetches the object or all sibling objects if there are multiple
    public RawFetchResponse fetch(String bucket, String key, RequestMeta meta) {
        return doFetch(bucket, key, meta, false);
    }

    public RawFetchResponse fetch(String bucket, String key) {
        return doFetch(bucket, key, null, false);
    }

    /**
     * Similar to fetch(), except the HTTP connection is left open, and the Riak
     * response is provided as a stream and processed on request. The user must
     * remember to call RawFetchResponse.close() on the return value.
     * 
     * @param bucket
     *            The bucket containing the {@link RiakObject} to fetch.
     * @param key
     *            The key of the {@link RiakObject} to fetch.
     * @param meta
     *            Extra metadata to attach to the request such as an r- value
     *            for the request, HTTP headers, and other query parameters. See
     *            RequestMeta.readParams().
     * 
     * @return A streaming {@link RawFetchResponse} containing HTTP response
     *         information and the response stream. The stream is processed when
     *         has/getObject() or has/getSibling() is called, or the stream can
     *         be read using {@link RawFetchResponse}.getStream(). The HTTP
     *         connection must be closed manually by the user by calling
     *         {@link RawFetchResponse}.close().
     */
    public RawFetchResponse stream(String bucket, String key, RequestMeta meta) {
        return doFetch(bucket, key, meta, true);
    }

    public RawFetchResponse stream(String bucket, String key) {
        return doFetch(bucket, key, null, true);
    }

    private RawFetchResponse doFetch(String bucket, String key, RequestMeta meta, boolean streamResponse) {
        if (meta == null) {
            meta = new RequestMeta();
        }

        String accept = meta.getQueryParam(Constants.HDR_ACCEPT);
        if (accept == null) {
            meta.setHeader(Constants.HDR_ACCEPT, Constants.CTYPE_ANY + ", " + Constants.CTYPE_MULTIPART_MIXED);
        } else {
            meta.setHeader(Constants.HDR_ACCEPT, accept + ", " + Constants.CTYPE_MULTIPART_MIXED);
        }

        HttpResponse r = helper.fetch(bucket, key, meta, streamResponse);
        if (r == null)
            return null;

        try {
            return new RawFetchResponse(r);
        } catch (RiakResponseException e) {
            return new RawFetchResponse(helper.toss(e));
        }

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
        HttpResponse r = helper.walk(bucket, key, walkSpec, meta);
        if (r == null)
            return null;

        try {
            return new RawWalkResponse(r);
        } catch (RiakResponseException e) {
            return new RawWalkResponse(helper.toss(e));
        }
    }

    public RawWalkResponse walk(String bucket, String key, String walkSpec) {
        return walk(bucket, key, walkSpec, null);
    }

    public RawWalkResponse walk(String bucket, String key, RiakWalkSpec walkSpec) {
        return walk(bucket, key, walkSpec.toString(), null);
    }

    /** @return the installed exception handler or null if not installed */
    public RiakExceptionHandler getExceptionHandler() {
        return helper.getExceptionHandler();
    }

    /**
     * Install an exception handler. If an exception handler is provided, then
     * the Riak client will hand exceptions to the handler rather than throwing
     * them.
     */
    public void setExceptionHandler(RiakExceptionHandler exceptionHandler) {
        helper.setExceptionHandler(exceptionHandler);
    }
}
