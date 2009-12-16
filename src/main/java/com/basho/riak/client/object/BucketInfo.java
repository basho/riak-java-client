package com.basho.riak.client.object;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class BucketInfo {
    
    public Map<String, String> getSchema() { return schema; }
    private Map<String, String> schema = new HashMap<String, String>();

    public Collection<String> getKeys() { return keys; }
    private Collection<String> keys = new ArrayList<String>();
    
    public BucketInfo(Map<String, String> schema, Collection<String> keys) {
        if (schema != null) this.schema = schema;
        if (keys != null) this.keys = keys;
    }
    
    public void setSchema(String property, String value) {
        this.schema.put(property, value);
    }
}
