package com.basho.riak.client.response;

import java.util.List;

import com.basho.riak.client.RiakObject;

public interface WalkResponse {
    public List<? extends List<? extends RiakObject>> getSteps();
}
