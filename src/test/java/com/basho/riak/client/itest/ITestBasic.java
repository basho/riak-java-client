package com.basho.riak.client.itest;

import static org.junit.Assert.*;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import com.basho.riak.client.jiak.JiakClient;
import com.basho.riak.client.jiak.JiakFetchResponse;
import com.basho.riak.client.jiak.JiakObject;
import com.basho.riak.client.jiak.JiakStoreResponse;
import com.basho.riak.client.raw.RawBucketInfo;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RawFetchResponse;
import com.basho.riak.client.raw.RawObject;
import com.basho.riak.client.raw.RawStoreResponse;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.response.HttpResponse;

/**
 * Basic exercises such as store, fetch, and modify objects for both the Raw and
 * Jiak clients. This assumes that Riak is reachable at 127.0.0.1:8098 with
 * raw_name and jiak_name set to "raw" and "jiak".
 * 
 */
public class ITestBasic {

    private static String RAW_URL = "http://127.0.0.1:8098/raw";
    private static String RAW_VALUE1 = "raw_value1";
    private static String RAW_VALUE2 = "raw_value2";
    private static String RAW_USERMETA_KEY = "usermeta";
    private static String RAW_USERMETA_VALUE = "value";
    private static String RAW_BUCKET = "raw_itest";
    private static String RAW_KEY = "raw_key";

    private static String JIAK_URL = "http://127.0.0.1:8098/raw";
    private static String JIAK_VALUE1 = "{\"jiak_value1\":1}";
    private static String JIAK_VALUE2 = "{\"jiak_value2\":2}";
    private static String JIAK_USERMETA_KEY = "usermeta";
    private static String JIAK_USERMETA_VALUE = "value";
    private static String JIAK_BUCKET = "jiak_itest";
    private static String JIAK_KEY = "jiak_key";

    private static RequestMeta DELETE_ALL_REPLICAS = RequestMeta.writeParams(null, 3);

    @Test
    public void store_fetch_modify_from_raw() {
        RawClient c = new RawClient(RAW_URL);

        // Set bucket schema to return siblings
        RawBucketInfo bucketInfo = new RawBucketInfo();
        bucketInfo.setAllowMult(true);
        HttpResponse bucketresp = c.setBucketSchema(RAW_BUCKET, bucketInfo);
        assertTrue(bucketresp.isSuccess());

        // Make sure object doesn't exist
        HttpResponse deleteresp = c.delete(RAW_BUCKET, RAW_KEY, DELETE_ALL_REPLICAS);
        assertTrue(deleteresp.isSuccess());

        RawFetchResponse fetchresp = c.fetch(RAW_BUCKET, RAW_KEY);
        assertEquals(404, fetchresp.getStatusCode());

        // Store a new object
        RawObject o = new RawObject(RAW_BUCKET, RAW_KEY, RAW_VALUE1);
        RawStoreResponse storeresp = c.store(o);
        assertTrue(storeresp.isSuccess());

        // Retrieve it back
        fetchresp = c.fetch(RAW_BUCKET, RAW_KEY);
        assertTrue(fetchresp.isSuccess());
        assertTrue(fetchresp.hasObject());
        assertEquals(RAW_VALUE1, fetchresp.getObject().getValue());
        assertTrue(fetchresp.getObject().getUsermeta().isEmpty());
        
        // Modify and store it
        o = fetchresp.getObject();
        o.setValue(RAW_VALUE2);
        o.getUsermeta().put(RAW_USERMETA_KEY, RAW_USERMETA_VALUE);
        storeresp = c.store(o);
        assertTrue(storeresp.isSuccess());

        // Validate modification happened
        fetchresp = c.fetch(RAW_BUCKET, RAW_KEY);
        assertTrue(fetchresp.isSuccess());
        assertTrue(fetchresp.hasObject());
        assertFalse(fetchresp.hasSiblings());
        assertEquals(RAW_VALUE2, fetchresp.getObject().getValue());
        assertEquals(RAW_USERMETA_VALUE, fetchresp.getObject().getUsermeta().get(RAW_USERMETA_KEY));
    }

    @Test
    public void store_fetch_modify_from_jiak() throws JSONException {
        JiakClient c = new JiakClient(JIAK_URL);

        // Make sure object doesn't exist
        HttpResponse deleteresp = c.delete(JIAK_BUCKET, JIAK_KEY, DELETE_ALL_REPLICAS);
        assertTrue(deleteresp.isSuccess());

        JiakFetchResponse fetchresp = c.fetch(JIAK_BUCKET, JIAK_KEY);
        assertEquals(404, fetchresp.getStatusCode());

        // Store a new object
        JiakObject o = new JiakObject(JIAK_BUCKET, JIAK_KEY, new JSONObject(JIAK_VALUE1));
        JiakStoreResponse storeresp = c.store(o);
        assertTrue(storeresp.isSuccess());

        // Retrieve it back
        fetchresp = c.fetch(JIAK_BUCKET, JIAK_KEY);
        assertTrue(fetchresp.isSuccess());
        assertTrue(fetchresp.hasObject());
        assertEquals(JIAK_VALUE1, fetchresp.getObject().getValue());
        assertTrue(fetchresp.getObject().getUsermeta().isEmpty());
        
        // Modify and store it
        o = fetchresp.getObject();
        o.setValue(new JSONObject(JIAK_VALUE2));
        o.getUsermetaAsJSON().put(JIAK_USERMETA_KEY, JIAK_USERMETA_VALUE);
        storeresp = c.store(o);
        assertTrue(storeresp.isSuccess());

        // Validate modification happened
        fetchresp = c.fetch(JIAK_BUCKET, JIAK_KEY);
        assertTrue(fetchresp.isSuccess());
        assertTrue(fetchresp.hasObject());
        assertEquals(JIAK_VALUE2, fetchresp.getObject().getValue());
        assertEquals(JIAK_USERMETA_VALUE, fetchresp.getObject().getUsermeta().get(JIAK_USERMETA_KEY));
    }
}
