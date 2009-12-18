package com.basho.riak.client.response;

import com.basho.riak.client.RiakBucketInfo;

/**
 * Response from a GET request at a bucket's URL
 */
public interface BucketResponse extends HttpResponse {

    /**
     * @return Whether the bucket's schema and keys were returned in the
     *         response from Riak
     */
    public boolean hasBucketInfo();

    /**
     * @return The bucket's schema and keys
     */
    public RiakBucketInfo getBucketInfo();

}
