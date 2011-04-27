package com.basho.riak.newapi;

import java.io.IOException;

import com.basho.riak.client.raw.Command;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.newapi.bucket.Bucket;
import com.basho.riak.newapi.bucket.FetchBucket;
import com.basho.riak.newapi.bucket.WriteBucket;
import com.basho.riak.newapi.cap.DefaultRetrier;
import com.basho.riak.newapi.query.BucketKeyMapReduce;
import com.basho.riak.newapi.query.BucketMapReduce;
import com.basho.riak.newapi.query.LinkWalk;

/**
 * @author russell
 * 
 */
public final class DefaultClient implements RiakClient {

    private final RawClient client;

    /**
     * @param client
     */
    DefaultClient(RawClient client) {
        this.client = client;
    }

    // BUCKET OPS

    public WriteBucket updateBucket(Bucket b) {
        WriteBucket op = new WriteBucket(client, b);
        return op;
    }

    public FetchBucket fetchBucket(String bucketName) {
        FetchBucket op = new FetchBucket(client, bucketName);
        return op;
    }

    public WriteBucket createBucket(String bucketName) {
        WriteBucket op = new WriteBucket(client, bucketName);
        return op;
    }

    // CLIENT ID

    public RiakClient setClientId(final byte[] clientId) throws RiakException {
        if (clientId == null || clientId.length != 4) {
            throw new IllegalArgumentException("Client Id must be 4 bytes long");
        }
        final byte[] cloned = clientId.clone();
        new DefaultRetrier().attempt(new Command<Void>() {
            public Void execute() throws IOException {
                client.setClientId(cloned);
                return null;
            }
        }, 3);

        return this;
    }

    public byte[] generateAndSetClientId() throws RiakException {
        final byte[] clientId = new DefaultRetrier().attempt(new Command<byte[]>() {
            public byte[] execute() throws IOException {
                return client.generateAndSetClientId();
            }
        }, 3);

        return clientId;
    }

    public byte[] getClientId() throws RiakException {
        final byte[] clientId = new DefaultRetrier().attempt(new Command<byte[]>() {
            public byte[] execute() throws IOException {
                return client.getClientId();
            }
        }, 3);

        return clientId;
    }

    // QUERY

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

    public LinkWalk walk(RiakObject startObject) {
        return null;
    }
}