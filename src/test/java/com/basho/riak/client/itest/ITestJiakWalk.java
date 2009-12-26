package com.basho.riak.client.itest;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import com.basho.riak.client.jiak.JiakClient;
import com.basho.riak.client.jiak.JiakObject;
import com.basho.riak.client.jiak.JiakWalkResponse;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.request.RiakWalkSpec;

public class ITestJiakWalk {
 
    public static String JIAK_URL = "http://127.0.0.1:8098/jiak";
    public static RequestMeta WRITE_3_REPLICAS() { return RequestMeta.writeParams(null, 3); }

    @Test
    public void test_jiak_walk() throws JSONException {
        final JiakClient c = new JiakClient(JIAK_URL);
        final String BUCKET = "test_jiak_walk";
        final String ROOT = "root";
        final String LEAF1 = "leaf1";
        final String LEAF2 = "leaf2";
        final String EXCLUDED_LEAF = "excluded_leaf";
        final String INCLUDED_VALUE = "{\"state\":\"included\"}";
        final String EXCLUDED_VALUE = "{\"state\":\"excluded\"}";
        final String TAG_INCLUDE = "tag_include";
        final String TAG_EXCLUDE = "tag_exclude";

        // Clear out the objects we're testing with
        assertTrue(c.delete(BUCKET, ROOT, WRITE_3_REPLICAS()).isSuccess());
        assertTrue(c.delete(BUCKET, LEAF1, WRITE_3_REPLICAS()).isSuccess());
        assertTrue(c.delete(BUCKET, LEAF2, WRITE_3_REPLICAS()).isSuccess());
        assertTrue(c.delete(BUCKET, EXCLUDED_LEAF, WRITE_3_REPLICAS()).isSuccess());

        // Add a few objects
        JiakObject leaf1 = new JiakObject(BUCKET, LEAF1, new JSONObject(INCLUDED_VALUE));
        JiakObject leaf2 = new JiakObject(BUCKET, LEAF2, new JSONObject(INCLUDED_VALUE));
        JiakObject excludedLeaf = new JiakObject(BUCKET, EXCLUDED_LEAF, new JSONObject(EXCLUDED_VALUE));
        JiakObject root = new JiakObject(BUCKET, ROOT);
        root.getLinksAsJSON().put(Arrays.asList(BUCKET, LEAF1, TAG_INCLUDE));
        root.getLinksAsJSON().put(Arrays.asList(BUCKET, LEAF2, TAG_INCLUDE));
        root.getLinksAsJSON().put(Arrays.asList(BUCKET, EXCLUDED_LEAF, TAG_EXCLUDE));
        assertTrue(c.store(root, WRITE_3_REPLICAS()).isSuccess());
        assertTrue(c.store(leaf1, WRITE_3_REPLICAS()).isSuccess());
        assertTrue(c.store(leaf2, WRITE_3_REPLICAS()).isSuccess());
        assertTrue(c.store(excludedLeaf, WRITE_3_REPLICAS()).isSuccess());

        // Perform walk
        RiakWalkSpec walkSpec = new RiakWalkSpec();
        walkSpec.addStep(BUCKET, TAG_INCLUDE, "1");
        JiakWalkResponse walkresp = c.walk(BUCKET, ROOT, walkSpec);
        assertTrue(walkresp.isSuccess() && walkresp.hasSteps());
        assertEquals(1, walkresp.getSteps().size());
        assertEquals(2, walkresp.getSteps().get(0).size());

        // Verify expected only linked to objects are returned
        List<? extends List<JiakObject>> steps = walkresp.getSteps();
        List<String> keys = new ArrayList<String>();
        for (List<JiakObject> step : steps) {
            for (JiakObject object : step) {
                keys.add(object.getKey());
                assertEquals("included", object.getValueAsJSON().optString("state"));
            }
        }
        assertTrue(keys.contains(LEAF1));
        assertTrue(keys.contains(LEAF2));
    }

}
