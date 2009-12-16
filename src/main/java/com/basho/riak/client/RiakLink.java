package com.basho.riak.client;

public class RiakLink {

    public RiakLink(String bucket, String key, String tag) {
        this.bucket = bucket;
        this.key = key;
        this.tag = tag;
    }
    
    public String getBucket() { return bucket; }
    public void setBucket(String bucket) { this.bucket = bucket; }
    private String bucket;

    public String getKey() { return key; }
    public void setKey(String key) { this.key = key; }
    private String key;
    
    public String getTag() { return tag; }
    public void setTag(String tag) { this.tag = tag; }
    private String tag;

}
