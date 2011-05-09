package com.basho.riak.client;

import java.util.concurrent.Callable;

import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.bucket.FetchBucket;
import com.basho.riak.client.bucket.WriteBucket;
import com.basho.riak.client.cap.DefaultRetrier;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.query.BucketKeyMapReduce;
import com.basho.riak.client.query.BucketMapReduce;
import com.basho.riak.client.query.LinkWalk;
import com.basho.riak.client.raw.RawClient;

/**
 * A default implementation of IRiakClient.
 *
 * Provides convenient, transport agnostic ways to perform
 * bucket and query operations on Riak.
 *
 * @author russell
 * 
 */
public final class DefaultRiakClient implements IRiakClient {

    private final RawClient rawClient;
    private final Retrier retrier;

    /**
     * @param rawClient
     * @param defaultRetrier
     */
    DefaultRiakClient(final RawClient rawClient, final Retrier defaultRetrier) {
        this.rawClient = rawClient;
        this.retrier = defaultRetrier;
    }

    /**
     * @param client
     */
    DefaultRiakClient(final RawClient rawClient) {
        this(rawClient, DefaultRetrier.attempts(3));
    }

    // BUCKET OPS

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakClient#updateBucket(com.basho.riak.client.bucket.Bucket)
     */
    public WriteBucket updateBucket(final Bucket b) {
        return new WriteBucket(rawClient, b.getName(), retrier);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakClient#fetchBucket(java.lang.String)
     */
    public FetchBucket fetchBucket(String bucketName) {
        return new FetchBucket(rawClient, bucketName, retrier);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakClient#createBucket(java.lang.String)
     */
    public WriteBucket createBucket(String bucketName) {
        return new WriteBucket(rawClient, bucketName, retrier);
    }

    // CLIENT ID

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakClient#setClientId(byte[])
     */
    public IRiakClient setClientId(final byte[] clientId) throws RiakException {
        if (clientId == null || clientId.length != 4) {
            throw new IllegalArgumentException("Client Id must be 4 bytes long");
        }
        final byte[] cloned = clientId.clone();
        retrier.attempt(new Callable<Void>() {
            public Void call() throws Exception {
                rawClient.setClientId(cloned);
                return null;
            }
        });

        return this;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakClient#generateAndSetClientId()
     */
    public byte[] generateAndSetClientId() throws RiakException {
        final byte[] clientId = retrier.attempt(new Callable<byte[]>() {
            public byte[] call() throws Exception {
                return rawClient.generateAndSetClientId();
            }
        });

        return clientId;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakClient#getClientId()
     */
    public byte[] getClientId() throws RiakException {
        final byte[] clientId = retrier.attempt(new Callable<byte[]>() {
            public byte[] call() throws Exception {
                return rawClient.getClientId();
            }
        });

        return clientId;
    }

    // QUERY

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakClient#mapReduce()
     */
    public BucketKeyMapReduce mapReduce() {
        return new BucketKeyMapReduce(rawClient);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.basho.riak.newapi.RiakClient#mapReduce(java.lang.String)
     */
    public BucketMapReduce mapReduce(String bucket) {
        return new BucketMapReduce(rawClient, bucket);
    }

    /*
     * (non-Javadoc)
     * @see com.basho.riak.client.IRiakClient#walk(com.basho.riak.client.IRiakObject)
     */
    public LinkWalk walk(IRiakObject startObject) {
        return new LinkWalk(rawClient, startObject);
    }
}