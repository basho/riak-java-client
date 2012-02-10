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

import static com.basho.riak.client.http.Hosts.RIAK_URL;
import static com.basho.riak.client.http.itest.Utils.*;
import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.junit.Test;

import com.basho.riak.client.AllTests;
import com.basho.riak.client.http.RiakBucketInfo;
import com.basho.riak.client.http.RiakClient;
import com.basho.riak.client.http.RiakObject;
import com.basho.riak.client.http.response.BucketResponse;
import com.basho.riak.client.http.response.FetchResponse;
import com.basho.riak.client.http.util.ClientUtils;
import com.basho.riak.client.http.util.Constants;
import com.basho.riak.client.raw.http.HTTPClientAdapter;
import com.basho.riak.client.util.CharsetUtils;

/**
 * Assumes Riak is reachable at {@link com.basho.riak.client.http.Hosts#RIAK_URL }.
 * @see com.basho.riak.client.http.Hosts#RIAK_URL
 */
public class ITestStreaming {

    @Test public void stream_keys() {
        final RiakClient c = new RiakClient(RIAK_URL);
        final String BUCKET = "test_stream_keys";
        final int NUM_KEYS = 20;
        
        // Add objects
        for (int i = 0; i < NUM_KEYS; i++) {
            assertSuccess(c.store(new RiakObject(BUCKET, "key" + Integer.toString(i), CharsetUtils.utf8StringToBytes("v")),
                  WRITE_3_REPLICAS()));
        }

        // Iterate over keys in bucket
        BucketResponse r = c.streamBucket(BUCKET);
        assertSuccess(r);

        List<String> keys = new ArrayList<String>();
        for (String key : r.getBucketInfo().getKeys()) {
            keys.add(key);
        }
        for (int i = 0; i < NUM_KEYS; i++) {
            assertTrue("Should contain key" + i + ": " + keys, keys.contains("key" + i));
        }
        
        r.close();

        // Clear out the objects we're testing with
        for (int i = 0; i < NUM_KEYS; i++) {
            assertSuccess(c.delete(BUCKET, "key" + Integer.toString(i)));
        }
    }

    @Test public void stream_object() throws IOException {
        final RiakClient c = new RiakClient(RIAK_URL);
        final String BUCKET = "test_stream_object";
        final String KEY = "key";
        final byte[] bytes = new byte[512];
        new Random().nextBytes(bytes);
        
        // Add object
        assertSuccess(c.store(new RiakObject(BUCKET, KEY, bytes), WRITE_3_REPLICAS()));

        // Stream the object back
        FetchResponse r = c.stream(BUCKET, KEY);
        assertSuccess(r);
        
        assertTrue(r.isStreamed());
        assertNotNull(r.getStream());
        
        ByteArrayOutputStream os = new ByteArrayOutputStream(512);
        ClientUtils.copyStream(r.getStream(), os);
        assertArrayEquals(bytes, os.toByteArray());
        
        r.close();

        AllTests.emptyBucket(BUCKET, new HTTPClientAdapter(c));
    }

    @Test public void stream_siblings() throws Exception {
        final RiakClient c = new RiakClient(RIAK_URL);
        final String BUCKET = "test_stream_siblings";
        final String KEY = UUID.randomUUID().toString();
        final byte[][] bytes = new byte[][] { new byte[512], new byte[512], new byte[512] };
        new Random().nextBytes(bytes[0]);
        new Random().nextBytes(bytes[1]);
        new Random().nextBytes(bytes[2]);
        
        // Enable storing siblings in this bucket
        BucketResponse bucketresp = c.listBucket(BUCKET);
        assertSuccess(bucketresp);
        assertTrue(bucketresp.hasBucketInfo());
        RiakBucketInfo bucketInfo = bucketresp.getBucketInfo();
        bucketInfo.setAllowMult(true);
        assertSuccess(c.setBucketSchema(BUCKET, bucketInfo));
        // allow the bucket properties to propagate around the ring
        Thread.sleep(1000);
        
        // Clean out any previous entry
        c.delete(BUCKET, KEY, WRITE_3_REPLICAS());
        
        // Add conflicting objects
        c.setClientId("foo-");
        assertSuccess(c.store(new RiakObject(BUCKET, KEY, bytes[0]), WRITE_3_REPLICAS().setQueryParam(Constants.QP_RETURN_BODY, "false")));
        
        c.setClientId("bar-");
        assertSuccess(c.store(new RiakObject(BUCKET, KEY, bytes[1]), WRITE_3_REPLICAS().setQueryParam(Constants.QP_RETURN_BODY, "false")));

        c.setClientId("baz-");
        assertSuccess(c.store(new RiakObject(BUCKET, KEY, bytes[2]), WRITE_3_REPLICAS().setQueryParam(Constants.QP_RETURN_BODY, "false")));

        // Stream the object back
        FetchResponse r = c.stream(BUCKET, KEY);
        assertEquals(300, r.getStatusCode());
        
        assertTrue(r.isStreamed());
        assertNotNull(r.getStream());
        
        assertTrue(r.hasSiblings());
        for (Iterator<RiakObject> i = r.getSiblings().iterator(); i.hasNext(); ) {
            RiakObject o = i.next();
            ByteArrayOutputStream os = new ByteArrayOutputStream(512);
            ClientUtils.copyStream(o.getValueStream(), os);
            byte[] out = os.toByteArray();
            boolean found = false;
            for (int j = 0; j < bytes.length; j++) {
                found = true;
                for (int k = 0; k < bytes[j].length; k++) {
                    if (bytes[j][k] != out[k]) {
                        found = false;
                        break;
                    }
                }
                if (found) {
                    break;
                }
            }
            assertTrue("Sibling value does not match any expected input", found);
        }
        
        r.close();
        RiakBucketInfo cleanUpBucketIfo = new RiakBucketInfo();
        cleanUpBucketIfo.setAllowMult(false);
        c.setBucketSchema(BUCKET, cleanUpBucketIfo);
        AllTests.emptyBucket(BUCKET, new HTTPClientAdapter(c));
    }
}
