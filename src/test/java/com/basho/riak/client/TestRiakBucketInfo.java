package com.basho.riak.client;

import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

public class TestRiakBucketInfo {

    RiakBucketInfo impl;
    JSONObject schema = new JSONObject();
    List<String> keys = new ArrayList<String>();
    
    
    @Before public void setup() {
        impl = new RiakBucketInfo(schema, keys); 
    }
    
    @Test public void constructor_schema_and_keys_returned_by_accessors() {
        assertSame(schema, impl.getSchema());
        assertSame(keys, impl.getKeys());
    }
    
    @Test public void objects_constructed_for_null_constructor_arguments() {
        impl = new RiakBucketInfo(null, null);
        assertNotNull(impl.getSchema());
        assertNotNull(impl.getKeys());
    }
}
