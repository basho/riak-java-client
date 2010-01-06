package com.basho.riak.client.itest;

import static com.basho.riak.client.itest.Utils.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.basho.riak.client.RiakLink;
import com.basho.riak.client.raw.RawBucketInfo;
import com.basho.riak.client.raw.RawBucketResponse;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RawFetchResponse;
import com.basho.riak.client.raw.RawObject;
import com.basho.riak.client.raw.RawStoreResponse;
import com.basho.riak.client.request.RequestMeta;

/**
 * Basic exercises such as store, fetch, and modify objects for the Raw client.
 * Assumes Riak is reachable at 127.0.0.1:8098/raw.
 */
public class ITestRawBasic {

    public static String RAW_URL = "http://127.0.0.1:8098/raw";
    public static RequestMeta WRITE_3_REPLICAS() { return RequestMeta.writeParams(null, 3); }

    @Test
    public void store_fetch_modify_from_raw() {
        final RawClient c = new RawClient(RAW_URL);
        final String VALUE1 = "value1";
        final String VALUE2 = "value2";
        final RiakLink LINK = new RiakLink("bucket", "key", "tag");
        final String USERMETA_KEY = "usermeta";
        final String USERMETA_VALUE = "value";
        final String BUCKET = "store_fetch_modify_from_raw";
        final String KEY = "key";

        // Set bucket schema to return siblings
        RawBucketInfo bucketInfo = new RawBucketInfo();
        bucketInfo.setAllowMult(true);
        assertSuccess(c.setBucketSchema(BUCKET, bucketInfo));

        // Make sure object doesn't exist
        assertSuccess(c.delete(BUCKET, KEY, WRITE_3_REPLICAS()));

        RawFetchResponse fetchresp = c.fetch(BUCKET, KEY);
        assertEquals(404, fetchresp.getStatusCode());

        // Store a new object
        RawObject o = new RawObject(BUCKET, KEY, VALUE1);
        RawStoreResponse storeresp = c.store(o, WRITE_3_REPLICAS());
        assertSuccess(storeresp);

        // Retrieve it back
        fetchresp = c.fetch(BUCKET, KEY);
        assertSuccess(fetchresp);
        assertTrue(fetchresp.hasObject());
        assertEquals(VALUE1, fetchresp.getObject().getValue());
        assertTrue(fetchresp.getObject().getLinks().isEmpty());
        assertTrue(fetchresp.getObject().getUsermeta().isEmpty());

        // Modify and store it
        o = fetchresp.getObject();
        o.setValue(VALUE2);
        o.getLinks().add(LINK);
        o.getUsermeta().put(USERMETA_KEY, USERMETA_VALUE);
        storeresp = c.store(o);
        assertSuccess(storeresp);

        // Validate modification happened
        fetchresp = c.fetch(BUCKET, KEY);
        assertSuccess(fetchresp);
        assertTrue(fetchresp.hasObject());
        assertFalse(fetchresp.hasSiblings());
        assertEquals(VALUE2, fetchresp.getObject().getValue());
        assertEquals(1, fetchresp.getObject().getLinks().size());
        assertEquals(LINK, fetchresp.getObject().getLinks().get(0));
        assertEquals(USERMETA_VALUE, fetchresp.getObject().getUsermeta().get(USERMETA_KEY));
    }

    @Test
    public void test_raw_bucket_schema() {
        final RawClient c = new RawClient(RAW_URL);
        final String BUCKET = "test_raw_bucket_schema";
        final String KEY1 = "key1";
        final String KEY2 = "key2";
        final String KEY3 = "key3";
        final String CHASH_MOD = "riak_util";
        final String CHASH_FUN = "chash_bucketonly_keyfun";

        // Clear out the objects we're testing with
        assertSuccess(c.delete(BUCKET, KEY1));
        assertSuccess(c.delete(BUCKET, KEY2));
        assertSuccess(c.delete(BUCKET, KEY3));

        // Add a few objects
        assertSuccess(c.store(new RawObject(BUCKET, KEY1, "v")));
        assertSuccess(c.store(new RawObject(BUCKET, KEY2, "v")));
        assertSuccess(c.store(new RawObject(BUCKET, KEY3, "v")));

        // Get the current bucket schema and contents
        RawBucketResponse bucketresp = c.listBucket(BUCKET);
        assertSuccess(bucketresp);
        assertTrue(bucketresp.hasBucketInfo());
        RawBucketInfo bucketInfo = bucketresp.getBucketInfo();
        int nval = bucketInfo.getNVal();

        // Verify that contents are correct
        assertTrue(bucketInfo.getKeys().contains(KEY1));
        assertTrue(bucketInfo.getKeys().contains(KEY2));
        assertTrue(bucketInfo.getKeys().contains(KEY3));

        // Change some properties
        bucketInfo.setNVal(nval + 1);
        bucketInfo.setCHashFun(CHASH_MOD, CHASH_FUN);
        assertSuccess(c.setBucketSchema(BUCKET, bucketInfo));

        // Verify that properties stuck
        bucketresp = c.listBucket(BUCKET);
        assertSuccess(bucketresp);
        assertTrue(bucketresp.hasBucketInfo());
        bucketInfo = bucketresp.getBucketInfo();
        assertEquals(nval + 1, bucketInfo.getNVal().intValue());
        assertEquals(CHASH_MOD + ":" + CHASH_FUN, bucketInfo.getCHashFun());
    }
}
