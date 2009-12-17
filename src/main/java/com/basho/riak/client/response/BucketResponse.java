package com.basho.riak.client.response;

import com.basho.riak.client.RiakBucketInfo;

public interface BucketResponse extends HttpResponse {

    public RiakBucketInfo getBucketInfo();

}
