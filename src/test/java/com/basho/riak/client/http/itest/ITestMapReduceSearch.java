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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.basho.riak.client.http.RiakBucketInfo;
import com.basho.riak.client.http.RiakClient;
import com.basho.riak.client.http.RiakObject;
import com.basho.riak.client.http.mapreduce.JavascriptFunction;
import com.basho.riak.client.http.response.BucketResponse;
import com.basho.riak.client.http.response.MapReduceResponse;
import com.basho.riak.client.http.response.StoreResponse;

/**
 * Exercises search map/reduce features of the Riak client. Assumes Riak is
 * reachable at {@link com.basho.riak.client.Hosts#RIAK_URL }.
 * 
 * @see com.basho.riak.client.Hosts#RIAK_URL
 */
public class ITestMapReduceSearch {
    public static enum Digit {
        ZERO, ONE, TWO, THREE, FOUR, FIVE, SIX, SEVEN, EIGHT, NINE
    };

    public static String SEARCH_BUCKET_NAME = "mr_stest_java";
    public static int TEST_ITEMS = 200;

    @BeforeClass public static void setup() {
        RiakClient c = new RiakClient(RIAK_URL);

        indexBucket(c, SEARCH_BUCKET_NAME);

        for (int i = 0; i < TEST_ITEMS; i++) {
            RiakObject searchObject = new RiakObject(SEARCH_BUCKET_NAME, "java_" + Integer.toString(i));
            searchObject.setContentType("application/json");
            searchObject.setValue("{\"foo\":\"" + Digit.values()[i % 10].toString().toLowerCase() + "\"}");
            StoreResponse sr = c.store(searchObject);
            assertTrue("Failed to set up test: " + sr.getBodyAsString(), sr.isSuccess());
        }
    }

    private static void indexBucket(RiakClient c, String bucket) {
        try {
            BucketResponse response = c.getBucketSchema(SEARCH_BUCKET_NAME);
            RiakBucketInfo info = response.getBucketInfo();
            JSONObject schema = info.getSchema();

            JSONArray precommit = (JSONArray) schema.get("precommit");
            for (int i = 0; i < precommit.length(); i++) {
                JSONObject handler = (JSONObject) precommit.get(i);
                if (handler.has("mod") && handler.get("mod").equals("riak_search_kv_hook"))
                    return;
            }
            precommit.put(new JSONObject() {
                {
                    put("mod", "riak_search_kv_hook");
                    put("fun", "precommit");
                }
            });

            c.setBucketSchema(SEARCH_BUCKET_NAME, info);
        } catch (JSONException e) {
            throw new RuntimeException("Should always be able to create JSONObject");
        }
    }

    @AfterClass public static void teardown() {
        RiakClient c = new RiakClient(RIAK_URL);
        for (int i = 0; i < TEST_ITEMS; i++) {
            c.delete(SEARCH_BUCKET_NAME, "java_" + Integer.toString(i));
        }
    }

    @Test public void doSearchMapOnly() throws IOException, JSONException {
        RiakClient c = new RiakClient(RIAK_URL);
        MapReduceResponse response = c.mapReduceOverSearch(SEARCH_BUCKET_NAME, "foo:zero")
                                        .map(JavascriptFunction.anon("function(v) { return [v]; }"), true)
                                            .submit();
        assertTrue(response.isSuccess());
        JSONArray results = response.getResults();
        assertEquals(TEST_ITEMS / 10, results.length());
    }
}