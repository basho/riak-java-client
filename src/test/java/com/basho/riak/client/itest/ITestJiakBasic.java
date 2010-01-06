package com.basho.riak.client.itest;

import static com.basho.riak.client.itest.Utils.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import com.basho.riak.client.RiakLink;
import com.basho.riak.client.jiak.JiakBucketInfo;
import com.basho.riak.client.jiak.JiakBucketResponse;
import com.basho.riak.client.jiak.JiakClient;
import com.basho.riak.client.jiak.JiakFetchResponse;
import com.basho.riak.client.jiak.JiakObject;
import com.basho.riak.client.jiak.JiakStoreResponse;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.util.Constants;

/**
 * Basic exercises such as store, fetch, and modify objects for the Jiak client.
 * Assumes Riak is reachable at 127.0.0.1:8098/jiak.
 */
public class ITestJiakBasic {

    public static String JIAK_URL = "http://127.0.0.1:8098/jiak";

    @Test
    public void store_fetch_modify_from_jiak() throws JSONException {
        final JiakClient c = new JiakClient(JIAK_URL);
        final String VALUE1 = "{\"value1\":1}";
        final String VALUE2 = "{\"value2\":2}";
        final RiakLink LINK = new RiakLink("bucket", "key", "tag");
        final String USERMETA_KEY = "usermeta";
        final String USERMETA_VALUE = "value";
        final String BUCKET = "jiak_itest";
        final String KEY = "jiak_key";

        // Make sure object doesn't exist
        HttpResponse deleteresp = c.delete(BUCKET, KEY, WRITE_3_REPLICAS());
        assertSuccess(deleteresp);

        JiakFetchResponse fetchresp = c.fetch(BUCKET, KEY);
        assertEquals(404, fetchresp.getStatusCode());

        // Store a new object
        JiakObject o = new JiakObject(BUCKET, KEY, new JSONObject(VALUE1));
        JiakStoreResponse storeresp = c.store(o);
        assertSuccess(storeresp);

        // Retrieve it back
        fetchresp = c.fetch(BUCKET, KEY);
        assertSuccess(fetchresp);
        assertTrue(fetchresp.hasObject());
        assertEquals(VALUE1, fetchresp.getObject().getValue().replaceAll("\\s", ""));
        assertTrue(fetchresp.getObject().getUsermeta().isEmpty());

        // Modify and store it
        o = fetchresp.getObject();
        o.setValue(new JSONObject(VALUE2));
        o.getLinks().add(LINK);
        o.getUsermeta().put(USERMETA_KEY, USERMETA_VALUE);
        storeresp = c.store(o);
        assertSuccess(storeresp);

        // Validate modification happened
        fetchresp = c.fetch(BUCKET, KEY);
        assertSuccess(fetchresp);
        assertTrue(fetchresp.hasObject());
        assertEquals(2, fetchresp.getObject().getValueAsJSON().optInt("value2"));
        assertEquals(1, fetchresp.getObject().getLinks().size());
        assertEquals(LINK, fetchresp.getObject().getLinks().get(0));
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
        assertSuccess(c.delete(BUCKET, KEY1));
        assertSuccess(c.delete(BUCKET, KEY2));
        assertSuccess(c.delete(BUCKET, KEY3));

        // Add a few objects
        assertSuccess(c.store(new JiakObject(BUCKET, KEY1, VALUE)));
        assertSuccess(c.store(new JiakObject(BUCKET, KEY2, VALUE)));
        assertSuccess(c.store(new JiakObject(BUCKET, KEY3, VALUE)));

        // Get the current bucket schema and contents
        JiakBucketResponse bucketresp = c.listBucket(BUCKET);
        assertSuccess(bucketresp);
        assertTrue(bucketresp.hasBucketInfo());
        JiakBucketInfo bucketInfo = bucketresp.getBucketInfo();

        // Verify that contents are correct
        assertTrue(bucketInfo.getKeys().contains(KEY1));
        assertTrue(bucketInfo.getKeys().contains(KEY2));
        assertTrue(bucketInfo.getKeys().contains(KEY3));

        // Set some properties
        bucketInfo.setAllowedFields(null);
        bucketInfo.setRequiredFields(null);
        assertSuccess(c.setBucketSchema(BUCKET, bucketInfo));

        // Verify that properties stuck
        bucketresp = c.listBucket(BUCKET);
        assertSuccess(bucketresp);
        assertTrue(bucketresp.hasBucketInfo());
        bucketInfo = bucketresp.getBucketInfo();
        assertEquals("*", bucketInfo.getSchema().optString(Constants.JIAK_FL_SCHEMA_ALLOWED_FIELDS));
        assertEquals(0, bucketInfo.getSchema().optJSONArray(Constants.JIAK_FL_SCHEMA_REQUIRED_FIELDS).length());

        // Change the same properties and reverify
        bucketInfo.setAllowedFields(ALLOWED_FIELDS);
        bucketInfo.setRequiredFields(REQUIRED_FIELDS);
        assertSuccess(c.setBucketSchema(BUCKET, bucketInfo));
        bucketInfo = c.listBucket(BUCKET).getBucketInfo();
        assertEquals(new JSONArray(ALLOWED_FIELDS).toString(),
                     bucketInfo.getSchema().optJSONArray(Constants.JIAK_FL_SCHEMA_ALLOWED_FIELDS).toString());
        assertEquals(new JSONArray(REQUIRED_FIELDS).toString(),
                     bucketInfo.getSchema().optJSONArray(Constants.JIAK_FL_SCHEMA_REQUIRED_FIELDS).toString());

    }
}
