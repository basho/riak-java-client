package com.basho.riak.client.response;

import com.basho.riak.client.RiakObject;

public interface FetchResponse {
    public RiakObject getObject();
    public boolean hasObject();
}
