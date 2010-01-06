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
package com.basho.riak.client.plain;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

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
import com.basho.riak.client.response.RiakIORuntimeException;
import com.basho.riak.client.response.RiakResponseRuntimeException;
import com.basho.riak.client.response.StoreResponse;
import com.basho.riak.client.response.StreamHandler;
import com.basho.riak.client.response.WalkResponse;

/**
 * An adapter from {@link RiakClient} to a slightly less HTTP, more
 * Java-centric, interface. Objects are returned without HTTP specific
 * information and exceptions are thrown on unsuccessful responses.
 */
public class PlainClient {

    private RiakClient impl;

    /** Connect to the Jiak interface using the given configuration. */
    public static PlainClient connectToJiak(RiakConfig config) {
        return new PlainClient(new JiakClient(config));
    }

    /** Connect to the Jiak interface using the given URL. */
    public static PlainClient connectToJiak(String url) {
        return new PlainClient(new JiakClient(url));
    }

    /** Connect to the Raw interface using the given configuration. */
    public static PlainClient connectToRaw(RiakConfig config) {
        return new PlainClient(new RawClient(config));
    }

    /** Connect to the Jiak interface using the given URL. */
    public static PlainClient connectToRaw(String url) {
        return new PlainClient(new RawClient(url));
    }

    /**
     * Object client wraps an existing {@link RiakClient} and adapts its
     * interface
     */
    public PlainClient(RiakClient riakClient) {
        impl = riakClient;
    }

    /**
     * See {@link RiakClient}.setBucketSchema().
     * 
     * @throws {@link RiakIOResponseException} If an error occurs during
     *         communication with the Riak server.
     * @throws {@link RiakResponseException} if the server does not successfully
     *         update the bucket schema.
     */
    public void setBucketSchema(String bucket, RiakBucketInfo bucketInfo, RequestMeta meta) throws RiakIOException,
            RiakResponseException {
        HttpResponse r = null;
        try {
            r = impl.setBucketSchema(bucket, bucketInfo, meta);
        } catch (RiakIORuntimeException ioe) {
            throw new RiakIOException(ioe);
        }

        if (r.getStatusCode() != 204)
            throw new RiakResponseException(new RiakResponseRuntimeException(r, r.getBody()));
    }

    public void setBucketSchema(String bucket, RiakBucketInfo bucketInfo) throws RiakIOException, RiakResponseException {
        setBucketSchema(bucket, bucketInfo, null);
    }

    /**
     * See {@link RiakClient}.listBucket().
     * 
     * @throws {@link RiakIOResponseException} If an error occurs during
     *         communication with the Riak server.
     * @throws {@link RiakResponseException} if the server does not return the
     *         bucket information
     */
    public RiakBucketInfo listBucket(String bucket, RequestMeta meta) throws RiakIOException, RiakResponseException {
        BucketResponse r;

        try {
            r = impl.listBucket(bucket, meta);
        } catch (RiakIORuntimeException ioe) {
            throw new RiakIOException(ioe);
        } catch (RiakResponseRuntimeException re) {
            throw new RiakResponseException(re);
        }

        if (r.getStatusCode() != 200)
            throw new RiakResponseException(new RiakResponseRuntimeException(r, r.getBody()));

        return r.getBucketInfo();
    }

    public RiakBucketInfo listBucket(String bucket) throws RiakIOException, RiakResponseException {
        return listBucket(bucket, null);
    }

    /**
     * See {@link RiakClient}.store().
     * 
     * @throws {@link RiakIOResponseException} If an error occurs during
     *         communication with the Riak server.
     * @throws {@link RiakResponseException} If the server does not succesfully
     *         store the object.
     */
    public void store(RiakObject object, RequestMeta meta) throws RiakIOException, RiakResponseException {
        StoreResponse r;
        try {
            r = impl.store(object, meta);
        } catch (RiakIORuntimeException ioe) {
            throw new RiakIOException(ioe);
        } catch (RiakResponseRuntimeException re) {
            throw new RiakResponseException(re);
        }

        if (r.getStatusCode() != 200 && r.getStatusCode() != 204)
            throw new RiakResponseException(new RiakResponseRuntimeException(r, r.getBody()));

        object.updateMeta(r);
    }

    public void store(RiakObject object) throws RiakIOException, RiakResponseException {
        store(object, null);
    }

    /**
     * See {@link RiakClient}.fetchMeta().
     * 
     * @returns {@link RiakObject} or null if object doesn't exist.
     * 
     * @throws {@link RiakIOResponseException} If an error occurs during
     *         communication with the Riak server.
     * @throws {@link RiakResponseException} If the server does return a
     *         valid object
     */
    public RiakObject fetchMeta(String bucket, String key, RequestMeta meta) throws RiakIOException,
            RiakResponseException {
        FetchResponse r;
        try {
            r = impl.fetchMeta(bucket, key, meta);
        } catch (RiakIORuntimeException ioe) {
            throw new RiakIOException(ioe);
        } catch (RiakResponseRuntimeException re) {
            throw new RiakResponseException(re);
        }

        if (r.getStatusCode() == 404)
            return null;

        if (r.getStatusCode() != 200 && r.getStatusCode() != 304)
            throw new RiakResponseException(new RiakResponseRuntimeException(r, r.getBody()));

        if (r.getStatusCode() == 200 && !r.hasObject())
            throw new RiakResponseException(new RiakResponseRuntimeException(r, "Failed to parse metadata"));

        return r.getObject();
    }

    public RiakObject fetchMeta(String bucket, String key) throws RiakIOException, RiakResponseException {
        return fetchMeta(bucket, key, null);
    }

    /**
     * See {@link RiakClient}.fetch().
     * 
     * @returns {@link RiakObject} or null if object doesn't exist. If siblings
     *          exist, then returns one of the siblings.
     * @throws {@link RiakIOResponseException} If an error occurs during
     *         communication with the Riak server.
     * @throws {@link RiakResponseException} If the server does return a
     *         valid object
     */
    public RiakObject fetch(String bucket, String key, RequestMeta meta) throws RiakIOException, RiakResponseException {
        FetchResponse r;
        try {
            r = impl.fetch(bucket, key, meta);
        } catch (RiakIORuntimeException ioe) {
            throw new RiakIOException(ioe);
        } catch (RiakResponseRuntimeException re) {
            throw new RiakResponseException(re);
        }

        if (r.getStatusCode() == 404)
            return null;

        if (r.getStatusCode() != 200 && r.getStatusCode() != 304)
            throw new RiakResponseException(new RiakResponseRuntimeException(r, r.getBody()));

        if (r.getStatusCode() == 200 && !r.hasObject())
            throw new RiakResponseException(new RiakResponseRuntimeException(r, "Failed to parse object"));

        return r.getObject();
    }

    public RiakObject fetch(String bucket, String key) throws RiakIOException, RiakResponseException {
        return fetch(bucket, key, null);
    }

    /**
     * See {@link RiakClient}.fetch().
     * 
     * @returns All sibling {@link RiakObject} or null if object doesn't exist.
     * @throws {@link RiakIOResponseException} If an error occurs during
     *         communication with the Riak server.
     * @throws {@link RiakResponseException} If the server does return any
     *         valid objects
     */
    public Collection<? extends RiakObject> fetchAll(String bucket, String key, RequestMeta meta)
            throws RiakIOException, RiakResponseException {
        FetchResponse r;
        try {
            r = impl.fetch(bucket, key, meta);
        } catch (RiakIORuntimeException ioe) {
            throw new RiakIOException(ioe);
        } catch (RiakResponseRuntimeException re) {
            throw new RiakResponseException(re);
        }

        if (r.getStatusCode() == 404)
            return null;

        if (r.getStatusCode() != 200 && r.getStatusCode() != 304)
            throw new RiakResponseException(new RiakResponseRuntimeException(r, r.getBody()));

        if (r.getStatusCode() == 200 && !(r.hasObject() || r.hasSiblings()))
            throw new RiakResponseException(new RiakResponseRuntimeException(r, "Failed to parse object"));

        if (r.hasSiblings())
            return r.getSiblings();
        return Arrays.asList(r.getObject());
    }

    public Collection<? extends RiakObject> fetchAll(String bucket, String key) throws RiakIOException,
            RiakResponseException {
        return fetchAll(bucket, key, null);
    }

    /**
     * See {@link RiakClient}.stream()
     */
    public boolean stream(String bucket, String key, StreamHandler handler, RequestMeta meta) throws IOException {
        return impl.stream(bucket, key, handler, meta);
    }

    /**
     * See {@link RiakClient}.delete(). Succeeds if object did not previously
     * exist.
     * 
     * @throws {@link RiakIOResponseException} If an error occurs during
     *         communication with the Riak server.
     * @throws {@link RiakResponseException} If the object was not deleted.
     */
    public void delete(String bucket, String key, RequestMeta meta) throws RiakIOException, RiakResponseException {
        HttpResponse r;
        try {
            r = impl.delete(bucket, key, meta);
        } catch (RiakIORuntimeException ioe) {
            throw new RiakIOException(ioe);
        }

        if (r.getStatusCode() != 204 && r.getStatusCode() != 404)
            throw new RiakResponseException(new RiakResponseRuntimeException(r, r.getBody()));
    }

    public void delete(String bucket, String key) throws RiakIOException, RiakResponseException {
        delete(bucket, key, null);
    }

    /**
     * See {@link RiakClient}.walk().
     * 
     * @returns list of lists of {@link RiakObject}s corresponding to steps of
     *          the walk. Returns null if the source object doesn't exist.
     * @throws {@link RiakIOResponseException} If an error occurs during
     *         communication with the Riak server.
     * @throws {@link RiakResponseException} If the links could not be walked or
     *         the result steps were not returned.
     */
    public List<? extends List<? extends RiakObject>> walk(String bucket, String key, String walkSpec, RequestMeta meta)
            throws RiakIOException, RiakResponseException {
        WalkResponse r;
        try {
            r = impl.walk(bucket, key, walkSpec, meta);
        } catch (RiakIORuntimeException ioe) {
            throw new RiakIOException(ioe);
        } catch (RiakResponseRuntimeException re) {
            throw new RiakResponseException(re);
        }

        if (r.getStatusCode() == 404)
            return null;

        if (r.getStatusCode() != 200)
            throw new RiakResponseException(new RiakResponseRuntimeException(r, r.getBody()));

        if (!r.hasSteps())
            throw new RiakResponseException(new RiakResponseRuntimeException(r, "Failed to parse walk results"));

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
