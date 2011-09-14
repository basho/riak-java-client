/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.client.itest;

import static com.basho.riak.client.itest.Utils.*;
import static org.junit.Assert.*;

import java.util.UUID;

import org.junit.Test;

import com.basho.riak.client.RiakBucketInfo;
import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.response.BucketResponse;
import com.basho.riak.client.response.FetchResponse;
import com.basho.riak.client.response.StoreResponse;
import com.basho.riak.client.util.Constants;

/**
 * Basic exercises such as store, fetch, and modify objects for the Riak client.
 * Assumes Riak is reachable at 127.0.0.1:8098/riak.
 */
public class ITestBasic {

    public static String RIAK_URL = "http://127.0.0.1:8098/riak";
    
    @Test public void store_fetch_modify() {
        final RiakClient c = new RiakClient(RIAK_URL);
        final String VALUE1 = "value1";
        final String VALUE2 = "value2";
        final RiakLink LINK = new RiakLink("bucket", "key", "tag");
        final String USERMETA_KEY = "usermeta";
        final String USERMETA_VALUE = "value";
        final String BUCKET = "store_fetch_modify";
        final String KEY = "key";

        // Set bucket schema to return siblings
        RiakBucketInfo bucketInfo = new RiakBucketInfo();
        bucketInfo.setAllowMult(true);
        assertSuccess(c.setBucketSchema(BUCKET, bucketInfo));

        // Make sure object doesn't exist
        assertSuccess(c.delete(BUCKET, KEY, WRITE_3_REPLICAS()));

        FetchResponse fetchresp = c.fetch(BUCKET, KEY);
        assertEquals(404, fetchresp.getStatusCode());

        // Store a new object
        RiakObject o = new RiakObject(BUCKET, KEY, VALUE1.getBytes());
        StoreResponse storeresp = c.store(o, WRITE_3_REPLICAS());
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
        storeresp = c.store(o, WRITE_3_REPLICAS());
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

    @Test public void test_bucket_schema() {
        final RiakClient c = new RiakClient(RIAK_URL);
        final String BUCKET = "test_bucket_schema";
        final String KEY1 = "key1";
        final String KEY2 = "key2";
        final String KEY3 = "key3";
        final String CHASH_MOD = "riak_core_util";
        final String CHASH_FUN = "chash_bucketonly_keyfun";
        
        // Clear out the objects we're testing with
        assertSuccess(c.delete(BUCKET, KEY1));
        assertSuccess(c.delete(BUCKET, KEY2));
        assertSuccess(c.delete(BUCKET, KEY3));

        // Add a few objects
        assertSuccess(c.store(new RiakObject(BUCKET, KEY1, "v".getBytes()), WRITE_3_REPLICAS()));
        assertSuccess(c.store(new RiakObject(BUCKET, KEY2, "v".getBytes()), WRITE_3_REPLICAS()));
        assertSuccess(c.store(new RiakObject(BUCKET, KEY3, "v".getBytes()), WRITE_3_REPLICAS()));

        // Get the current bucket schema and contents
        BucketResponse bucketresp = c.listBucket(BUCKET);
        assertSuccess(bucketresp);
        assertTrue(bucketresp.hasBucketInfo());
        RiakBucketInfo bucketInfo = bucketresp.getBucketInfo();
        int nval = bucketInfo.getNVal();

        // Verify that contents are correct
        assertTrue("Should contain key1: " + bucketInfo.getKeys(), bucketInfo.getKeys().contains(KEY1));
        assertTrue("Should contain key2: " + bucketInfo.getKeys(), bucketInfo.getKeys().contains(KEY2));
        assertTrue("Should contain key3: " + bucketInfo.getKeys(), bucketInfo.getKeys().contains(KEY3));

        // Change some properties
        bucketInfo.setNVal(nval + 1);
        bucketInfo.setCHashFun(CHASH_MOD, CHASH_FUN);
        assertSuccess(c.setBucketSchema(BUCKET, bucketInfo));

        // Verify that properties stuck
        bucketresp = c.getBucketSchema(BUCKET);
        assertSuccess(bucketresp);
        assertTrue(bucketresp.hasBucketInfo());
        bucketInfo = bucketresp.getBucketInfo();
        assertEquals(nval + 1, bucketInfo.getNVal().intValue());
    }

    @Test public void fetch_meta_with_siblings() {
		final RiakClient c1 = new RiakClient(RIAK_URL);
		final RiakClient c2 = new RiakClient(RIAK_URL);
		final String bucket = UUID.randomUUID().toString();
		final String key = UUID.randomUUID().toString();

		RiakBucketInfo bucketInfo = new RiakBucketInfo();
		bucketInfo.setAllowMult(true);
		assertSuccess(c1.setBucketSchema(bucket, bucketInfo));

		c1.store(new RiakObject(c1, bucket, key, "v".getBytes()));

		final FetchResponse fm1 = c1.fetchMeta(bucket, key);

		assertTrue(fm1.hasObject());
		assertNull(fm1.getObject().getValue());
		assertFalse(fm1.hasSiblings());

		final FetchResponse fr = c1.fetch(bucket, key);

		RiakObject ro = fr.getObject();

		RiakObject ro2 = new RiakObject(c2, bucket, key);
		ro2.copyData(ro);
		ro2.setValue("v2".getBytes());

		c1.store(ro);
		c2.store(ro2);

		FetchResponse fm2 = c1.fetchMeta(bucket, key);
		// since there are siblings fetchMeta will do a full fetch to get them
		assertTrue(fm2.hasObject());
		assertNotNull(fm2.getObject().getValue());
		assertTrue(fm2.hasSiblings());
	}
}
