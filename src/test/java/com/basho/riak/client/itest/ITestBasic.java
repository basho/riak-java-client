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

import static com.basho.riak.client.Hosts.RIAK_URL;
import static com.basho.riak.client.itest.Utils.*;
import static org.junit.Assert.*;

import java.util.UUID;

import org.junit.Test;

import com.basho.riak.client.RiakBucketInfo;
import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.response.BucketResponse;
import com.basho.riak.client.response.FetchResponse;
import com.basho.riak.client.response.StoreResponse;

/**
 * Basic exercises such as store, fetch, and modify objects for the Riak client.
 * Assumes Riak is reachable at {@link com.basho.riak.client.Hosts#RIAK_URL }.
 * @see com.basho.riak.client.Hosts#RIAK_URL
 */
public class ITestBasic {

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
        bucketInfo.setNVal(3);
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
        assertFalse(fetchresp.getObject().hasLinks());
        assertFalse(fetchresp.getObject().hasUsermeta());

        // Modify and store it
        o = fetchresp.getObject();
        o.setValue(VALUE2);
        o.addLink(LINK);
        o.addUsermetaItem(USERMETA_KEY, USERMETA_VALUE);
        storeresp = c.store(o, WRITE_3_REPLICAS());
        assertSuccess(storeresp);

        // Validate modification happened
        fetchresp = c.fetch(BUCKET, KEY);
        assertSuccess(fetchresp);
        assertTrue(fetchresp.hasObject());
        assertFalse(fetchresp.hasSiblings());
        assertEquals(VALUE2, fetchresp.getObject().getValue());
        assertEquals(1, fetchresp.getObject().numLinks());
        assertTrue(fetchresp.getObject().hasLink(LINK));
        assertEquals(USERMETA_VALUE, fetchresp.getObject().getUsermetaItem(USERMETA_KEY));
    }

    @Test public void test_bucket_schema() {
        final RiakClient c = new RiakClient(RIAK_URL);
        final String BUCKET = UUID.randomUUID().toString();
        final String KEY1 = UUID.randomUUID().toString();
        final String KEY2 = UUID.randomUUID().toString();
        final String KEY3 = UUID.randomUUID().toString();
        final String CHASH_MOD = "riak_core_util";
        final String CHASH_FUN = "chash_bucketonly_keyfun";

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

        // Clear out the objects we're testing with
        assertSuccess(c.delete(BUCKET, KEY1));
        assertSuccess(c.delete(BUCKET, KEY2));
        assertSuccess(c.delete(BUCKET, KEY3));
    }
}
