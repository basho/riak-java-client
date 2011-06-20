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

import static com.basho.riak.client.Hosts.RIAK_HOST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.httpclient.HttpException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.basho.riak.client.itest.ITestMapReduce.Digit;
import com.basho.riak.pbc.MapReduceResponseSource;
import com.basho.riak.pbc.RiakClient;
import com.basho.riak.pbc.RiakObject;
import com.basho.riak.pbc.mapreduce.ErlangFunction;
import com.basho.riak.pbc.mapreduce.JavascriptFunction;
import com.basho.riak.pbc.mapreduce.MapReduceBuilder;
import com.basho.riak.pbc.mapreduce.MapReduceResponse;

/**
 * Exercises map/reduce features of the Riak client.
 * Assumes Riak is reachable at {@link com.basho.riak.client.Hosts#RIAK_HOST }.
 * @see com.basho.riak.client.Hosts#RIAK_HOST
 */
public class ITestMapReduce {

    private static final String KEY_PREFIX = "pbc_java_";
    private static final String TAG = "test";
    public static String BUCKET = "pbc_mr_test_java";
	public static String	SEARCH_BUCKET_NAME	= "pbc_mr_stest_java";
    public static int TEST_ITEMS = 200;

    @BeforeClass public static void setup() throws Exception {
        final RiakClient c = new RiakClient(RIAK_HOST);
        for (int i = 0; i < TEST_ITEMS; i++) {
            RiakObject object = new RiakObject(BUCKET, KEY_PREFIX + Integer.toString(i), Integer.toString(i));
            object.setContentType("text/plain");

            if (i < TEST_ITEMS - 1) {
                object.addLink(TAG, BUCKET, KEY_PREFIX + Integer.toString(i + 1));
            }
            c.store(object);
            
			RiakObject searchObject = new RiakObject(SEARCH_BUCKET_NAME, "java_" + Integer.toString(i), "{\"foo\":\"" + Digit.values()[i % 10].toString().toLowerCase() + "\"}");
			searchObject.setContentType("application/json");
			c.store(searchObject);
        }
    }

    @AfterClass public static void teardown() throws Exception {
        final RiakClient c = new RiakClient(RIAK_HOST);

        for (int i = 0; i < TEST_ITEMS; i++) {
            c.delete(BUCKET, KEY_PREFIX + Integer.toString(i));
			c.delete(SEARCH_BUCKET_NAME, "java_" + Integer.toString(i));
        }
    }
    
	/**
	 * This test is fully functional but requires that the target bucket have search
	 * installed.  As the PBC client currently cannot access/change bucket schemas, there
	 * appears no way to accomplish this programatically without using the HTTP interface.
	 * 
	 * So for now I have this disabled - but the test will execute properly on
	 * an indexed bucket so if at some point the above limitation is addressed then this 
	 * test can be re-enabled.  For an example of this fully functional, see 
	 * {@link com.basho.riak.client.itest.ITestMapReduce}.
	 */
    @Ignore
	@Test
	public void doSearchMapOnly() throws HttpException, IOException, JSONException {
		RiakClient c = new RiakClient(RIAK_HOST);
        MapReduceBuilder mrb = new MapReduceBuilder(c)
    		.setBucket(SEARCH_BUCKET_NAME)
    		.setSearch("foo:zero")
    		.map(JavascriptFunction.anon("function(v) { return [v]; }"), true);
        MapReduceResponseSource response = mrb.submit();
        assertTrue(response.hasNext());
        int i = 0;
		JSONArray results;
		while (response.hasNext() && (results = response.next().getJSON()) != null) {
			i += results.length();
		}
		assertEquals(TEST_ITEMS / 10, i);
	}

    @Test public void doLinkMapReduce() throws HttpException, IOException, JSONException {
       final RiakClient c = new RiakClient(RIAK_HOST);

        MapReduceBuilder mrb = new MapReduceBuilder(c)
            .setBucket(BUCKET).link(BUCKET, TAG, false)
                .map(JavascriptFunction.named("Riak.mapValuesJson"), false)
                    .reduce(new ErlangFunction("riak_kv_mapreduce", "reduce_sort"), true);

        MapReduceResponseSource response = mrb.submit();

        assertTrue(response.hasNext());

        MapReduceResponse mrr = response.next();
        JSONArray results = mrr.getJSON();
        assertEquals(TEST_ITEMS - 1, results.length());
    }

    @Test public void doErlangMapReduce() throws HttpException, IOException, JSONException {
        RiakClient c = new RiakClient(RIAK_HOST);
        MapReduceBuilder builder = new MapReduceBuilder(c);
        builder.setBucket(BUCKET);
        builder.map(new ErlangFunction("riak_kv_mapreduce", "map_object_value"), false);
        builder.reduce(new ErlangFunction("riak_kv_mapreduce", "reduce_string_to_integer"), false);
        builder.reduce(new ErlangFunction("riak_kv_mapreduce", "reduce_sort"), true);
        MapReduceResponseSource response = builder.submit();
        assertTrue(response.hasNext());
        JSONArray results = response.next().getJSON();
        assertEquals(TEST_ITEMS, results.length());
        assertEquals(0, results.getInt(0));
        assertEquals(73, results.getInt(73));
        assertEquals(197, results.getInt(197));
    }

    @Test public void doJavascriptMapReduce() throws HttpException, IOException, JSONException {
        RiakClient c = new RiakClient(RIAK_HOST);
        MapReduceBuilder builder = new MapReduceBuilder(c);
        builder.setBucket(BUCKET);
        builder.map(JavascriptFunction.named("Riak.mapValuesJson"), false);
        builder.reduce(JavascriptFunction.named("Riak.reduceNumericSort"), true);
        MapReduceResponseSource response = builder.submit();
        assertTrue(response.hasNext());
        JSONArray results = response.next().getJSON();
        assertEquals(TEST_ITEMS, results.length());
        assertEquals(0, results.getInt(0));
        assertEquals(73, results.getInt(73));
        assertEquals(197, results.getInt(197));
    }

    @Test public void doJavascriptMapReduceFromJSON() throws HttpException, IOException, JSONException {
        RiakClient c = new RiakClient(RIAK_HOST);
        MapReduceBuilder builder = new MapReduceBuilder();
        builder.setBucket(BUCKET);
        builder.map(JavascriptFunction.named("Riak.mapValuesJson"), false);
        builder.reduce(JavascriptFunction.named("Riak.reduceNumericSort"), true);
        JSONObject mrJob = builder.toJSON();

        MapReduceResponseSource response = c.mapReduce(mrJob);
        assertTrue(response.hasNext());
        JSONArray results = response.next().getJSON();
        assertEquals(TEST_ITEMS, results.length());
        assertEquals(0, results.getInt(0));
        assertEquals(73, results.getInt(73));
        assertEquals(197, results.getInt(197));
    }
}
