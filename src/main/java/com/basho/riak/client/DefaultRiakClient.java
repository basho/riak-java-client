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

    private final RawClient client;
    private final Retrier retrier;

    /**
     * @param client
     * @param defaultRetrier
     */
    DefaultRiakClient(final RawClient client, final Retrier defaultRetrier) {
        this.client = client;
        this.retrier = defaultRetrier;
    }

    /**
     * @param client
     */
    DefaultRiakClient(final RawClient client) {
        this(client, new DefaultRetrier(3));
    }

    // BUCKET OPS

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakClient#updateBucket(com.basho.riak.client.bucket.Bucket)
     */
    public WriteBucket updateBucket(final Bucket b) {
        return new WriteBucket(client, b.getName(), retrier);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakClient#fetchBucket(java.lang.String)
     */
    public FetchBucket fetchBucket(String bucketName) {
        return new FetchBucket(client, bucketName, retrier);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakClient#createBucket(java.lang.String)
     */
    public WriteBucket createBucket(String bucketName) {
        return new WriteBucket(client, bucketName, retrier);
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
                client.setClientId(cloned);
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
                return client.generateAndSetClientId();
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
                return client.getClientId();
            }
        });

        return clientId;
    }

    // QUERY

    /* (non-Javadoc)
     * @see com.basho.riak.client.IRiakClient#mapReduce()
     */
    public BucketKeyMapReduce mapReduce() {
        return new BucketKeyMapReduce(client);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.basho.riak.newapi.RiakClient#mapReduce(java.lang.String)
     */
    public BucketMapReduce mapReduce(String bucket) {
        return new BucketMapReduce(client, bucket);
    }

    /*
     * (non-Javadoc)
     * @see com.basho.riak.client.IRiakClient#walk(com.basho.riak.client.IRiakObject)
     */
    public LinkWalk walk(IRiakObject startObject) {
        return new LinkWalk(client, startObject);
    }
}