package com.basho.riak.client.itest;

import static com.basho.riak.client.itest.Utils.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.basho.riak.client.RiakLink;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RawObject;
import com.basho.riak.client.raw.RawWalkResponse;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.request.RiakWalkSpec;

public class ITestRawWalk {
    
    public static String RAW_URL = "http://127.0.0.1:8098/raw";
    public static RequestMeta WRITE_3_REPLICAS() { return RequestMeta.writeParams(null, 3); }

    @Test
    public void test_raw_walk() {
        final RawClient c = new RawClient(RAW_URL);
        final String BUCKET = "test_raw_walk";
        final String ROOT = "root";
        final String LEAF1 = "leaf1";
        final String LEAF2 = "leaf2";
        final String EXCLUDED_LEAF = "excluded_leaf";
        final String INCLUDED_VALUE = "included";
        final String EXCLUDED_VALUE = "excluded";
        final String TAG_INCLUDE = "tag_include";
        final String TAG_EXCLUDE = "tag_exclude";

        // Clear out the objects we're testing with
        assertSuccess(c.delete(BUCKET, ROOT));
        assertSuccess(c.delete(BUCKET, LEAF1));
        assertSuccess(c.delete(BUCKET, LEAF2));
        assertSuccess(c.delete(BUCKET, EXCLUDED_LEAF));
        
        // Add a few objects
        RawObject leaf1 = new RawObject(BUCKET, LEAF1, INCLUDED_VALUE);
        RawObject leaf2 = new RawObject(BUCKET, LEAF2, INCLUDED_VALUE);
        RawObject excludedLeaf = new RawObject(BUCKET, EXCLUDED_LEAF, EXCLUDED_VALUE);
        RawObject root = new RawObject(BUCKET, ROOT);
        root.getLinks().add(new RiakLink(BUCKET, LEAF1, TAG_INCLUDE));
        root.getLinks().add(new RiakLink(BUCKET, LEAF2, TAG_INCLUDE));
        root.getLinks().add(new RiakLink(BUCKET, EXCLUDED_LEAF, TAG_EXCLUDE));
        assertSuccess(c.store(root, WRITE_3_REPLICAS()));
        assertSuccess(c.store(leaf1, WRITE_3_REPLICAS()));
        assertSuccess(c.store(leaf2, WRITE_3_REPLICAS()));
        assertSuccess(c.store(excludedLeaf, WRITE_3_REPLICAS()));
        
        // Perform walk
        RiakWalkSpec walkSpec = new RiakWalkSpec();
        walkSpec.addStep(BUCKET, TAG_INCLUDE, "1");
        RawWalkResponse walkresp = c.walk(BUCKET, ROOT, walkSpec);
        assertSuccess(walkresp);
        assertTrue(walkresp.hasSteps());
        assertEquals(1, walkresp.getSteps().size());
        assertEquals(2, walkresp.getSteps().get(0).size());
        
        // Verify expected only linked to objects are returned
        List<? extends List<RawObject>> steps = walkresp.getSteps();
        List<String> keys = new ArrayList<String>();
        for (List<RawObject> step : steps) {
            for (RawObject object : step) {
                keys.add(object.getKey());
                assertEquals(INCLUDED_VALUE, object.getValue());
            }
        }
        assertTrue(keys.contains(LEAF1));
        assertTrue(keys.contains(LEAF2));
    }
}
