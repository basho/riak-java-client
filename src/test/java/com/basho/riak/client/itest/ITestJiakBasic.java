package com.basho.riak.client.itest;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import com.basho.riak.client.jiak.JiakBucketInfo;
import com.basho.riak.client.jiak.JiakBucketResponse;
import com.basho.riak.client.jiak.JiakClient;
import com.basho.riak.client.jiak.JiakFetchResponse;
import com.basho.riak.client.jiak.JiakObject;
import com.basho.riak.client.jiak.JiakStoreResponse;
import com.basho.riak.client.jiak.JiakWalkResponse;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.request.RiakWalkSpec;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.util.Constants;

/**
 * Basic exercises such as store, fetch, and modify objects for the Jiak client.
 * Assumes Riak is reachable at 127.0.0.1:8098/jiak.
 */
public class ITestJiakBasic {

    private static String JIAK_URL = "http://127.0.0.1:8098/jiak";
    private static RequestMeta WRITE_3_REPLICAS = RequestMeta.writeParams(null, 3);

    @Test
    public void store_fetch_modify_from_jiak() throws JSONException {
        final JiakClient c = new JiakClient(JIAK_URL);
        final String VALUE1 = "{\"value1\":1}";
        final String VALUE2 = "{\"value2\":2}";
        final String LINK = "[\"bucket\",\"key\",\"tag\"]";
        final String USERMETA_KEY = "usermeta";
        final String USERMETA_VALUE = "value";
        final String BUCKET = "jiak_itest";
        final String KEY = "jiak_key";

        // Make sure object doesn't exist
        HttpResponse deleteresp = c.delete(BUCKET, KEY, WRITE_3_REPLICAS);
        assertTrue(deleteresp.isSuccess());

        JiakFetchResponse fetchresp = c.fetch(BUCKET, KEY);
        assertEquals(404, fetchresp.getStatusCode());

        // Store a new object
        JiakObject o = new JiakObject(BUCKET, KEY, new JSONObject(VALUE1));
        JiakStoreResponse storeresp = c.store(o);
        assertTrue(storeresp.isSuccess());

        // Retrieve it back
        fetchresp = c.fetch(BUCKET, KEY);
        assertTrue(fetchresp.isSuccess());
        assertTrue(fetchresp.hasObject());
        assertEquals(VALUE1, fetchresp.getObject().getValue().replaceAll("\\s", ""));
        assertTrue(fetchresp.getObject().getUsermeta().isEmpty());

        // Modify and store it
        o = fetchresp.getObject();
        o.setValue(new JSONObject(VALUE2));
        o.getLinksAsJSON().put(new JSONArray(LINK));
        o.getUsermetaAsJSON().put(USERMETA_KEY, USERMETA_VALUE);
        storeresp = c.store(o);
        assertTrue(storeresp.isSuccess());

        // Validate modification happened
        fetchresp = c.fetch(BUCKET, KEY);
        assertTrue(fetchresp.isSuccess());
        assertTrue(fetchresp.hasObject());
        assertEquals(2, fetchresp.getObject().getValueAsJSON().optInt("value2"));
        assertEquals(1, fetchresp.getObject().getLinks().size());
        assertEquals(LINK, fetchresp.getObject().getLinksAsJSON().get(0).toString().replaceAll("\\s", ""));
        assertEquals(USERMETA_VALUE, fetchresp.getObject().getUsermeta().get(USERMETA_KEY));
    }

    @Test
    public void test_jiak_bucket_schema() throws JSONException {
        final JiakClient c = new JiakClient(JIAK_URL);
        final String BUCKET = "test_jiak_bucket_schema";
        final String KEY1 = "key1";
        final String KEY2 = "key2";
        final String KEY3 = "key3";
        final JSONObject VALUE = new JSONObject("{\"value\":1}");
        final List<String> ALLOWED_FIELDS = Arrays.asList("value", "type");
        final List<String> REQUIRED_FIELDS = Arrays.asList("value");

        // Clear out the objects we're testing with
        assertTrue(c.delete(BUCKET, KEY1).isSuccess());
        assertTrue(c.delete(BUCKET, KEY2).isSuccess());
        assertTrue(c.delete(BUCKET, KEY3).isSuccess());

        // Add a few objects
        assertTrue(c.store(new JiakObject(BUCKET, KEY1, VALUE)).isSuccess());
        assertTrue(c.store(new JiakObject(BUCKET, KEY2, VALUE)).isSuccess());
        assertTrue(c.store(new JiakObject(BUCKET, KEY3, VALUE)).isSuccess());

        // Get the current bucket schema and contents
        JiakBucketResponse bucketresp = c.listBucket(BUCKET);
        assertTrue(bucketresp.isSuccess() && bucketresp.hasBucketInfo());
        JiakBucketInfo bucketInfo = bucketresp.getBucketInfo();

        // Verify that contents are correct
        assertTrue(bucketInfo.getKeys().contains(KEY1));
        assertTrue(bucketInfo.getKeys().contains(KEY2));
        assertTrue(bucketInfo.getKeys().contains(KEY3));

        // Set some properties
        bucketInfo.setAllowedFields(null);
        bucketInfo.setRequiredFields(null);
        assertTrue(c.setBucketSchema(BUCKET, bucketInfo).isSuccess());

        // Verify that properties stuck
        bucketresp = c.listBucket(BUCKET);
        assertTrue(bucketresp.isSuccess() && bucketresp.hasBucketInfo());
        bucketInfo = bucketresp.getBucketInfo();
        assertEquals("*", bucketInfo.getSchema().optString(Constants.JIAK_FL_SCHEMA_ALLOWED_FIELDS));
        assertEquals(0, bucketInfo.getSchema().optJSONArray(Constants.JIAK_FL_SCHEMA_REQUIRED_FIELDS).length());

        // Change the same properties and reverify
        bucketInfo.setAllowedFields(ALLOWED_FIELDS);
        bucketInfo.setRequiredFields(REQUIRED_FIELDS);
        assertTrue(c.setBucketSchema(BUCKET, bucketInfo).isSuccess());
        bucketInfo = c.listBucket(BUCKET).getBucketInfo();
        assertEquals(new JSONArray(ALLOWED_FIELDS).toString(),
                     bucketInfo.getSchema().optJSONArray(Constants.JIAK_FL_SCHEMA_ALLOWED_FIELDS).toString());
        assertEquals(new JSONArray(REQUIRED_FIELDS).toString(),
                     bucketInfo.getSchema().optJSONArray(Constants.JIAK_FL_SCHEMA_REQUIRED_FIELDS).toString());

    }

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
        assertTrue(c.delete(BUCKET, ROOT).isSuccess());
        assertTrue(c.delete(BUCKET, LEAF1).isSuccess());
        assertTrue(c.delete(BUCKET, LEAF2).isSuccess());
        assertTrue(c.delete(BUCKET, EXCLUDED_LEAF).isSuccess());

        // Add a few objects
        JiakObject leaf1 = new JiakObject(BUCKET, LEAF1, new JSONObject(INCLUDED_VALUE));
        JiakObject leaf2 = new JiakObject(BUCKET, LEAF2, new JSONObject(INCLUDED_VALUE));
        JiakObject excludedLeaf = new JiakObject(BUCKET, EXCLUDED_LEAF, new JSONObject(EXCLUDED_VALUE));
        JiakObject root = new JiakObject(BUCKET, ROOT);
        root.getLinksAsJSON().put(Arrays.asList(BUCKET, LEAF1, TAG_INCLUDE));
        root.getLinksAsJSON().put(Arrays.asList(BUCKET, LEAF2, TAG_INCLUDE));
        root.getLinksAsJSON().put(Arrays.asList(BUCKET, EXCLUDED_LEAF, TAG_EXCLUDE));
        assertTrue(c.store(root, WRITE_3_REPLICAS).isSuccess());
        assertTrue(c.store(leaf1, WRITE_3_REPLICAS).isSuccess());
        assertTrue(c.store(leaf2, WRITE_3_REPLICAS).isSuccess());
        assertTrue(c.store(excludedLeaf, WRITE_3_REPLICAS).isSuccess());

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
