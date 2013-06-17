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
package com.basho.riak.client.http.itest;

import static com.basho.riak.client.util.CharsetUtils.*;
import static com.basho.riak.client.http.Hosts.RIAK_URL;
import static com.basho.riak.client.http.itest.Utils.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.basho.riak.client.AllTests;
import com.basho.riak.client.http.RiakBucketInfo;
import com.basho.riak.client.http.RiakClient;
import com.basho.riak.client.http.RiakLink;
import com.basho.riak.client.http.RiakObject;
import com.basho.riak.client.http.plain.PlainClient;
import com.basho.riak.client.http.plain.RiakIOException;
import com.basho.riak.client.http.plain.RiakResponseException;
import com.basho.riak.client.http.request.RequestMeta;
import com.basho.riak.client.http.response.*;
import com.basho.riak.client.http.util.Constants;
import com.basho.riak.client.raw.http.HTTPClientAdapter;
import org.json.JSONObject;

/**
 * Basic exercises such as store, fetch, and modify objects for the Riak client.
 * Assumes Riak is reachable at {@link com.basho.riak.client.http.Hosts#RIAK_URL }.
 * @see com.basho.riak.client.http.Hosts#RIAK_URL
 */
public class ITestBasic {

    private String bucketName;

    @Before public void setUp() throws IOException {
        this.bucketName = this.getClass().getName();
    }

    @After public void tearDown() throws Exception {
        RiakBucketInfo bucketInfo = new RiakBucketInfo();
        bucketInfo.setAllowMult(false);
        bucketInfo.setNVal(3);
        new RiakClient(RIAK_URL).setBucketSchema(bucketName, bucketInfo);
        AllTests.emptyBucket(bucketName, new HTTPClientAdapter(new RiakClient(RIAK_URL)));
    }

    @Test public void ping() {
        final RiakClient c = new RiakClient(RIAK_URL);
        HttpResponse response = c.ping();
        assertSuccess(response);
    }

    @Test public void stats() {
        RiakClient c = new RiakClient(RIAK_URL);
        HttpResponse resp = c.stats();
        String json = resp.getBodyAsString();
        assertTrue(json.length() > 0);
    }
    
    @Test public void store_fetch_modify() {
        final RiakClient c = new RiakClient(RIAK_URL);
        final String VALUE1 = "value1";
        final String VALUE2 = "value2";
        final RiakLink LINK = new RiakLink("bucket", "key", "tag");
        final String USERMETA_KEY = "usermeta";
        final String USERMETA_VALUE = "value";
        final String BUCKET = bucketName;
        final String KEY = UUID.randomUUID().toString();

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
        RiakObject o = new RiakObject(BUCKET, KEY, utf8StringToBytes(VALUE1));
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

    @Test public void deleteQuorumIsApplied() {
        final RiakClient c = new RiakClient(RIAK_URL);

        final String key = UUID.randomUUID().toString();
        final byte[] value = utf8StringToBytes("value");

        RiakBucketInfo bucketInfo = new RiakBucketInfo();
        bucketInfo.setNVal(3);

        c.setBucketSchema(bucketName, bucketInfo);
        RiakObject o = new RiakObject(bucketName, key, value);

        final RequestMeta rm = WRITE_3_REPLICAS();

        StoreResponse storeresp = c.store(o, rm);
        assertSuccess(storeresp);

        HttpResponse deleteResponse = c.delete(bucketName, key, RequestMeta.deleteParams(4));

        assertEquals(400, deleteResponse.getStatusCode());
        assertTrue("Unexpected error message", deleteResponse.getBodyAsString().contains("Specified w/dw/pw values invalid for bucket n value of 3"));
    }

    @Test public void fetchNonExistant() {
        final RiakClient c = new RiakClient(RIAK_URL);
        final String KEY = UUID.randomUUID().toString();

        FetchResponse fetchresp = c.fetch(bucketName, KEY);
        assertEquals(404, fetchresp.getStatusCode());
        assertNull(fetchresp.getObject());
    }

    @Test public void fetchNonExistant_plainClient() throws RiakIOException, RiakResponseException {
        final RiakClient c = new RiakClient(RIAK_URL);
        PlainClient client = new PlainClient(c);
        final String KEY = UUID.randomUUID().toString();

        RiakObject o = client.fetch(bucketName, KEY);
        assertNull(o);
    }

    @Test public void fetchWithRGreaterN() throws RiakIOException, RiakResponseException {
        final RiakClient c = new RiakClient(RIAK_URL);
        final String key = UUID.randomUUID().toString();
        final byte[] value = utf8StringToBytes("value");

        RiakBucketInfo bucketInfo = new RiakBucketInfo();
        bucketInfo.setNVal(3);

        c.setBucketSchema(bucketName, bucketInfo);
        RiakObject o = new RiakObject(bucketName, key, value);

        final RequestMeta rm = WRITE_3_REPLICAS();

        StoreResponse storeresp = c.store(o, rm);
        assertSuccess(storeresp);

        try {
            c.fetch(bucketName, key, REQUIRE_7_REPLICAS());
            fail("Expected RiakResponseRuntimeException when R > N");
        } catch (RiakResponseRuntimeException re) {
            // ignore this because it means the test passed!
        }
    }

    @Test public void storeWithReturnBody() {
        final RiakClient c = new RiakClient(RIAK_URL);
        final String key = UUID.randomUUID().toString();
        final byte[] value = utf8StringToBytes("value");

        RiakObject o = new RiakObject(bucketName, key, value);
        StoreResponse storeresp = c.store(o, WRITE_3_REPLICAS());
        assertSuccess(storeresp);

        assertTrue(storeresp.hasObject());
        assertArrayEquals(value, storeresp.getObject().getValueAsBytes());
        assertFalse(storeresp.hasSiblings());
        assertEquals(0, storeresp.getSiblings().size());
    }

    @Test public void storeWithReturnBodyAndSiblings() throws InterruptedException {
        final RiakClient c = new RiakClient(RIAK_URL);

        final String key = UUID.randomUUID().toString();
        final byte[] value = utf8StringToBytes("value");
        final byte[] newValue = utf8StringToBytes("new_value");

        RiakBucketInfo bucketInfo = new RiakBucketInfo();
        bucketInfo.setAllowMult(true);

        c.setBucketSchema(bucketName, bucketInfo);
        Thread.sleep(1000);
        // allow the bucket properties to propagate around the ring

        RiakObject o = new RiakObject(bucketName, key, value);

        final RequestMeta rm = WRITE_3_REPLICAS().setHeader(Constants.QP_RETURN_BODY, "true");

        StoreResponse storeresp = c.store(o, rm);
        assertSuccess(storeresp);

        o = new RiakObject(bucketName, key, newValue);
        storeresp = c.store(o, rm);

        assertSuccess(storeresp);
        assertTrue(storeresp.hasObject());
        assertTrue(storeresp.hasSiblings());
    }

    @Test public void listBuckets() throws Exception {
        // we can't know about all the *other* buckets in riak
        // so create a couple and check that they are included in the response
        final RiakClient c = new RiakClient(RIAK_URL);

        final String bucket1 = UUID.randomUUID().toString();
        final String bucket2 = UUID.randomUUID().toString();
        final String key = UUID.randomUUID().toString();

        RiakObject o1 = new RiakObject(bucket1, key);
        StoreResponse storeresp = c.store(o1, WRITE_3_REPLICAS());
        assertSuccess(storeresp);

        RiakObject o2 = new RiakObject(bucket2, key);
        storeresp = c.store(o2, WRITE_3_REPLICAS());
        assertSuccess(storeresp);

        ListBucketsResponse resp = c.listBuckets(false);

        Set<String> buckets = resp.getBuckets();

        assertTrue(buckets.contains(bucket1));
        assertTrue(buckets.contains(bucket2));

        AllTests.emptyBucket(bucket1, new HTTPClientAdapter(c));
        AllTests.emptyBucket(bucket2, new HTTPClientAdapter(c));
    }

    @Test public void fetch_meta_with_siblings() throws Exception {
        final RiakClient c1 = new RiakClient(RIAK_URL);
        final RiakClient c2 = new RiakClient(RIAK_URL);
        final String key = UUID.randomUUID().toString();

        RiakBucketInfo bucketInfo = new RiakBucketInfo();
        bucketInfo.setAllowMult(true);
        assertSuccess(c1.setBucketSchema(bucketName, bucketInfo));
        // allow the bucket properties to propagate around the ring
        Thread.sleep(1000);

        c1.store(new RiakObject(c1, bucketName, key, "v".getBytes()));

        final FetchResponse fm1 = c1.fetchMeta(bucketName, key);

        assertTrue(fm1.hasObject());
        assertNull(fm1.getObject().getValue());
        assertFalse(fm1.hasSiblings());

        final FetchResponse fr = c1.fetch(bucketName, key);

        RiakObject ro = fr.getObject();

        RiakObject ro2 = new RiakObject(c2, bucketName, key);
        ro2.copyData(ro);
        ro2.setValue("v2".getBytes());

        c1.store(ro);
        c2.store(ro2);

        FetchResponse fm2 = c1.fetchMeta(bucketName, key);
        // since there are siblings fetchMeta will do a full fetch to get them
        assertTrue(fm2.hasSiblings());
        assertTrue(fm2.hasObject());
        assertNotNull(fm2.getObject().getValue());
    }
}
