package com.basho.riak.client;

import static org.junit.Assert.*;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

public class TestRiakBucketInfo {

    RiakBucketInfo impl;
    JSONObject schema = new JSONObject();
    JSONArray keys = new JSONArray();
    
    @Test public void constructor_schema_and_keys_returned_by_accessors() {
        final String KEY = "key";
        keys.put(KEY);

        impl = new RiakBucketInfo(schema, keys); 

        assertSame(schema, impl.getSchema());
        assertTrue(impl.getKeys().contains(KEY));
    }
    
    @Test public void objects_constructed_for_null_constructor_arguments() {
        impl = new RiakBucketInfo(null, null);
        assertNotNull(impl.getSchema());
        assertNotNull(impl.getKeys());
    }
}
