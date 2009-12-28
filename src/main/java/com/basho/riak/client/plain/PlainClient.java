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
import com.basho.riak.client.response.RiakIOException;
import com.basho.riak.client.response.RiakResponseException;
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
     * @throws {@link RiakPlainResponseException} if the server does not
     *         successfully update the bucket schema.
     */
    public void setBucketSchema(String bucket, RiakBucketInfo bucketInfo, RequestMeta meta)
            throws RiakPlainIOException, RiakPlainResponseException {
        HttpResponse r = null;
        try {
            r = impl.setBucketSchema(bucket, bucketInfo, meta);
        } catch (RiakIOException ioe) {
            throw new RiakPlainIOException(ioe);
        }

        if (r.getStatusCode() != 204)
            throw new RiakPlainResponseException(new RiakResponseException(r, r.getBody()));
    }

    public void setBucketSchema(String bucket, RiakBucketInfo bucketInfo) throws RiakPlainIOException,
            RiakPlainResponseException {
        setBucketSchema(bucket, bucketInfo, null);
    }

    /**
     * See {@link RiakClient}.listBucket().
     * 
     * @throws {@link RiakIOResponseException} If an error occurs during
     *         communication with the Riak server.
     * @throws {@link RiakPlainResponseException} if the server does not return
     *         the bucket information
     */
    public RiakBucketInfo listBucket(String bucket, RequestMeta meta) throws RiakPlainIOException,
            RiakPlainResponseException {
        BucketResponse r;

        try {
            r = impl.listBucket(bucket, meta);
        } catch (RiakIOException ioe) {
            throw new RiakPlainIOException(ioe);
        } catch (RiakResponseException re) {
            throw new RiakPlainResponseException(re);
        }

        if (r.getStatusCode() != 200)
            throw new RiakPlainResponseException(new RiakResponseException(r, r.getBody()));

        return r.getBucketInfo();
    }

    public RiakBucketInfo listBucket(String bucket) throws RiakPlainIOException, RiakPlainResponseException {
        return listBucket(bucket, null);
    }

    /**
     * See {@link RiakClient}.store().
     * 
     * @throws {@link RiakIOResponseException} If an error occurs during
     *         communication with the Riak server.
     * @throws {@link RiakPlainResponseException} If the server does not
     *         succesfully store the object.
     */
    public void store(RiakObject object, RequestMeta meta) throws RiakPlainIOException, RiakPlainResponseException {
        StoreResponse r;
        try {
            r = impl.store(object, meta);
        } catch (RiakIOException ioe) {
            throw new RiakPlainIOException(ioe);
        } catch (RiakResponseException re) {
            throw new RiakPlainResponseException(re);
        }

        if (r.getStatusCode() != 200 && r.getStatusCode() != 204)
            throw new RiakPlainResponseException(new RiakResponseException(r, r.getBody()));

        object.updateMeta(r);
    }

    public void store(RiakObject object) throws RiakPlainIOException, RiakPlainResponseException {
        store(object, null);
    }

    /**
     * See {@link RiakClient}.fetchMeta().
     * 
     * @returns {@link RiakObject} or null if object doesn't exist.
     * 
     * @throws {@link RiakIOResponseException} If an error occurs during
     *         communication with the Riak server.
     * @throws {@link RiakPlainResponseException} If the server does return the
     *         a valid object
     */
    public RiakObject fetchMeta(String bucket, String key, RequestMeta meta) throws RiakPlainIOException,
            RiakPlainResponseException {
        FetchResponse r;
        try {
            r = impl.fetchMeta(bucket, key, meta);
        } catch (RiakIOException ioe) {
            throw new RiakPlainIOException(ioe);
        } catch (RiakResponseException re) {
            throw new RiakPlainResponseException(re);
        }

        if (r.getStatusCode() == 404)
            return null;

        if (r.getStatusCode() != 200 && r.getStatusCode() != 304)
            throw new RiakPlainResponseException(new RiakResponseException(r, r.getBody()));

        if (r.getStatusCode() == 200 && !r.hasObject())
            throw new RiakPlainResponseException(new RiakResponseException(r, "Failed to parse metadata"));

        return r.getObject();
    }

    public RiakObject fetchMeta(String bucket, String key) throws RiakPlainIOException, RiakPlainResponseException {
        return fetchMeta(bucket, key, null);
    }

    /**
     * See {@link RiakClient}.fetch().
     * 
     * @returns {@link RiakObject} or null if object doesn't exist. If siblings
     *          exist, then returns one of the siblings.
     * @throws {@link RiakIOResponseException} If an error occurs during
     *         communication with the Riak server.
     * @throws {@link RiakPlainResponseException} If the server does return the
     *         a valid object
     */
    public RiakObject fetch(String bucket, String key, RequestMeta meta) throws RiakPlainIOException,
            RiakPlainResponseException {
        FetchResponse r;
        try {
            r = impl.fetch(bucket, key, meta);
        } catch (RiakIOException ioe) {
            throw new RiakPlainIOException(ioe);
        } catch (RiakResponseException re) {
            throw new RiakPlainResponseException(re);
        }

        if (r.getStatusCode() == 404)
            return null;

        if (r.getStatusCode() != 200 && r.getStatusCode() != 304)
            throw new RiakPlainResponseException(new RiakResponseException(r, r.getBody()));

        if (r.getStatusCode() == 200 && !r.hasObject())
            throw new RiakPlainResponseException(new RiakResponseException(r, "Failed to parse object"));

        return r.getObject();
    }

    public RiakObject fetch(String bucket, String key) throws RiakPlainIOException, RiakPlainResponseException {
        return fetch(bucket, key, null);
    }

    /**
     * See {@link RiakClient}.fetch().
     * 
     * @returns All sibling {@link RiakObject} or null if object doesn't exist.
     * @throws {@link RiakIOResponseException} If an error occurs during
     *         communication with the Riak server.
     * @throws {@link RiakPlainResponseException} If the server does return the
     *         a valid object
     */
    public Collection<? extends RiakObject> fetchAll(String bucket, String key, RequestMeta meta)
            throws RiakPlainIOException, RiakPlainResponseException {
        FetchResponse r;
        try {
            r = impl.fetch(bucket, key, meta);
        } catch (RiakIOException ioe) {
            throw new RiakPlainIOException(ioe);
        } catch (RiakResponseException re) {
            throw new RiakPlainResponseException(re);
        }

        Collection<? extends RiakObject> a = fetchAll("", "", null);
        for (RiakObject o : a) {
            o.getBucket();
        }

        if (r.getStatusCode() == 404)
            return null;

        if (r.getStatusCode() != 200 && r.getStatusCode() != 304)
            throw new RiakPlainResponseException(new RiakResponseException(r, r.getBody()));

        if (r.getStatusCode() == 200 && !r.hasObject())
            throw new RiakPlainResponseException(new RiakResponseException(r, "Failed to parse object"));

        if (r.hasSiblings())
            return r.getSiblings();
        return Arrays.asList(r.getObject());
    }

    public Collection<? extends RiakObject> fetchAll(String bucket, String key) throws RiakPlainIOException,
            RiakPlainResponseException {
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
     * @throws {@link RiakPlainResponseException} If the object was not deleted.
     */
    public void delete(String bucket, String key, RequestMeta meta) throws RiakPlainIOException,
            RiakPlainResponseException {
        HttpResponse r;
        try {
            r = impl.delete(bucket, key, meta);
        } catch (RiakIOException ioe) {
            throw new RiakPlainIOException(ioe);
        }

        if (r.getStatusCode() != 204 && r.getStatusCode() != 404)
            throw new RiakPlainResponseException(new RiakResponseException(r, r.getBody()));
    }

    public void delete(String bucket, String key) throws RiakPlainIOException, RiakPlainResponseException {
        delete(bucket, key, null);
    }

    /**
     * See {@link RiakClient}.walk().
     * 
     * @returns list of lists of {@link RiakObject}s corresponding to steps of
     *          the walk. Returns null if the source object doesn't exist.
     * @throws {@link RiakIOResponseException} If an error occurs during
     *         communication with the Riak server.
     * @throws {@link RiakPlainResponseException} If the links could not be
     *         walked or the result steps were not returned.
     */
    public List<? extends List<? extends RiakObject>> walk(String bucket, String key, String walkSpec, RequestMeta meta)
            throws RiakPlainIOException, RiakPlainResponseException {
        WalkResponse r;
        try {
            r = impl.walk(bucket, key, walkSpec, meta);
        } catch (RiakIOException ioe) {
            throw new RiakPlainIOException(ioe);
        } catch (RiakResponseException re) {
            throw new RiakPlainResponseException(re);
        }

        if (r.getStatusCode() == 404)
            return null;

        if (r.getStatusCode() != 200)
            throw new RiakPlainResponseException(new RiakResponseException(r, r.getBody()));

        if (!r.hasSteps())
            throw new RiakPlainResponseException(new RiakResponseException(r, "Failed to parse walk results"));

        return r.getSteps();
    }

    public List<? extends List<? extends RiakObject>> walk(String bucket, String key, String walkSpec)
            throws RiakPlainIOException, RiakPlainResponseException {
        return walk(bucket, key, walkSpec, null);
    }

    public List<? extends List<? extends RiakObject>> walk(String bucket, String key, RiakWalkSpec walkSpec)
            throws RiakPlainIOException, RiakPlainResponseException {
        return walk(bucket, key, walkSpec.toString(), null);
    }
}
