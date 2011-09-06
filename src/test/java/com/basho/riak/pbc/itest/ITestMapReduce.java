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
package com.basho.riak.pbc.itest;

import static com.basho.riak.pbc.MapReduceResponseSource.readAllResults;
import static com.basho.riak.client.http.Hosts.RIAK_HOST;
import static com.basho.riak.client.http.Hosts.RIAK_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.basho.riak.pbc.MapReduceResponseSource;
import com.basho.riak.pbc.RiakClient;
import com.basho.riak.pbc.RiakObject;
import com.basho.riak.pbc.mapreduce.ErlangFunction;
import com.basho.riak.pbc.mapreduce.JavascriptFunction;
import com.basho.riak.pbc.mapreduce.MapReduceBuilder;

/**
 * Exercises map/reduce features of the Riak client. Assumes Riak is reachable
 * at {@link com.basho.riak.client.http.Hosts#RIAK_HOST }.
 * 
 * @see com.basho.riak.client.http.Hosts#RIAK_HOST
 */
public class ITestMapReduce {

    private static final String KEY_PREFIX = "pbc_java_";
    private static final String TAG = "test";
    public static String BUCKET = "pbc_mr_test_java";
    public static int TEST_ITEMS = 200;

    @BeforeClass public static void setup() throws Exception {
        final RiakClient c = new RiakClient(RIAK_HOST, RIAK_PORT);
        for (int i = 0; i < TEST_ITEMS; i++) {
            RiakObject object = new RiakObject(BUCKET, KEY_PREFIX + Integer.toString(i), Integer.toString(i));
            object.setContentType("text/plain");

            if (i < TEST_ITEMS - 1) {
                object.addLink(TAG, BUCKET, KEY_PREFIX + Integer.toString(i + 1));
            }
            c.store(object);
        }
    }

    @AfterClass public static void teardown() throws Exception {
        final RiakClient c = new RiakClient(RIAK_HOST, RIAK_PORT);

        for (int i = 0; i < TEST_ITEMS; i++) {
            c.delete(BUCKET, KEY_PREFIX + Integer.toString(i));
        }
    }

    @Test public void doLinkMapReduce() throws IOException, JSONException {
        final RiakClient c = new RiakClient(RIAK_HOST, RIAK_PORT);

        MapReduceBuilder mrb = new MapReduceBuilder(c).
            setBucket(BUCKET).link(BUCKET, TAG, false)
            .map(JavascriptFunction.named("Riak.mapValuesJson"), false)
            .reduce(new ErlangFunction("riak_kv_mapreduce","reduce_sort"), true);

        MapReduceResponseSource response = mrb.submit();

        assertTrue(response.hasNext());

        JSONArray results = readAllResults(response);
        assertEquals(TEST_ITEMS - 1, results.length());
    }

    @Test public void doErlangMapReduce() throws IOException, JSONException {
        RiakClient c = new RiakClient(RIAK_HOST, RIAK_PORT);
        MapReduceBuilder builder = new MapReduceBuilder(c);
        builder.setBucket(BUCKET);
        builder.map(new ErlangFunction("riak_kv_mapreduce", "map_object_value"), false);
        builder.reduce(new ErlangFunction("riak_kv_mapreduce", "reduce_string_to_integer"), false);
        builder.reduce(new ErlangFunction("riak_kv_mapreduce", "reduce_sort"), true);
        MapReduceResponseSource response = builder.submit();
        assertTrue(response.hasNext());
        JSONArray results = readAllResults(response);
        assertEquals(TEST_ITEMS, results.length());
        assertEquals(0, results.getInt(0));
        assertEquals(73, results.getInt(73));
        assertEquals(197, results.getInt(197));
    }

    @Test public void doJavascriptMapReduce() throws IOException, JSONException {
        RiakClient c = new RiakClient(RIAK_HOST, RIAK_PORT);
        MapReduceBuilder builder = new MapReduceBuilder(c);
        builder.setBucket(BUCKET);
        builder.map(JavascriptFunction.named("Riak.mapValuesJson"), false);
        builder.reduce(JavascriptFunction.named("Riak.reduceNumericSort"), true);
        MapReduceResponseSource response = builder.submit();
        assertTrue(response.hasNext());
        JSONArray results = readAllResults(response);
        assertEquals(TEST_ITEMS, results.length());
        assertEquals(0, results.getInt(0));
        assertEquals(73, results.getInt(73));
        assertEquals(197, results.getInt(197));
    }

    @Test public void doJavascriptMapReduceFromJSON() throws IOException, JSONException {
        RiakClient c = new RiakClient(RIAK_HOST, RIAK_PORT);
        MapReduceBuilder builder = new MapReduceBuilder();
        builder.setBucket(BUCKET);
        builder.map(JavascriptFunction.named("Riak.mapValuesJson"), false);
        builder.reduce(JavascriptFunction.named("Riak.reduceNumericSort"), true);
        JSONObject mrJob = builder.toJSON();

        MapReduceResponseSource response = c.mapReduce(mrJob);
        assertTrue(response.hasNext());
        JSONArray results = readAllResults(response);
        assertEquals(TEST_ITEMS, results.length());
        assertEquals(0, results.getInt(0));
        assertEquals(73, results.getInt(73));
        assertEquals(197, results.getInt(197));
    }

    @Test public void doMultiPhaseResults() throws IOException, JSONException {
        RiakClient c = new RiakClient(RIAK_HOST, RIAK_PORT);
        MapReduceBuilder builder = new MapReduceBuilder();
        builder.setBucket(BUCKET);
        builder.map(JavascriptFunction.named("Riak.mapValuesJson"), true);
        builder.reduce(JavascriptFunction.named("Riak.reduceNumericSort"), true);
        JSONObject mrJob = builder.toJSON();

        MapReduceResponseSource response = c.mapReduce(mrJob);
        assertTrue(response.hasNext());
        JSONArray results = readAllResults(response);

        assertEquals(2, results.length());

        JSONArray unsorted = (JSONArray) results.get(0);
        JSONArray sorted = (JSONArray) results.get(1);

        assertEquals(TEST_ITEMS, unsorted.length());
        assertEquals(TEST_ITEMS, sorted.length());
        assertEquals(0, sorted.getInt(0));
        assertEquals(73, sorted.getInt(73));
        assertEquals(197, sorted.getInt(197));
    }
}
