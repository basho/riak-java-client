package com.basho.riak.client.response;

public interface StoreResponse {
    public String getVclock();
    public String getLastmod();
    public String getVtag();
}
