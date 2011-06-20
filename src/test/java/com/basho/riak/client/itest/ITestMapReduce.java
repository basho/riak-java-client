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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.httpclient.HttpException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.basho.riak.client.RiakBucketInfo;
import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.mapreduce.ErlangFunction;
import com.basho.riak.client.mapreduce.JavascriptFunction;
import com.basho.riak.client.request.MapReduceBuilder;
import com.basho.riak.client.response.BucketResponse;
import com.basho.riak.client.response.MapReduceResponse;

/**
 * Exercises map/reduce features of the Riak client. Assumes Riak is reachable at {@link com.basho.riak.client.Hosts#RIAK_URL }.
 * 
 * @see com.basho.riak.client.Hosts#RIAK_URL
 */
public class ITestMapReduce {
	public static enum Digit { ZERO, ONE, TWO, THREE, FOUR, FIVE, SIX, SEVEN, EIGHT, NINE };
	
	public static String	BUCKET_NAME	       = "mr_test_java";
	public static String	SEARCH_BUCKET_NAME	= "mr_stest_java";
	public static int	 TEST_ITEMS	           = 200;
	
	@BeforeClass
	public static void setup() {
		RiakClient c = new RiakClient(RIAK_URL);
		
		indexBucket(c, SEARCH_BUCKET_NAME);
		
		for (int i = 0; i < TEST_ITEMS; i++) {
			RiakObject object = new RiakObject(BUCKET_NAME, "java_" + Integer.toString(i));
			object.setContentType("text/plain");
			object.setValue(Integer.toString(i));
			if (i < TEST_ITEMS - 1) {
				RiakLink link = new RiakLink(BUCKET_NAME, "java_" + Integer.toString(i + 1), "test");
				List<RiakLink> links = new ArrayList<RiakLink>(1);
				links.add(link);
				object.setLinks(links);
			}
			c.store(object);
			
			RiakObject searchObject = new RiakObject(SEARCH_BUCKET_NAME, "java_" + Integer.toString(i));
			searchObject.setContentType("application/json");
			searchObject.setValue("{\"foo\":\"" + Digit.values()[i % 10].toString().toLowerCase() + "\"}");
			c.store(searchObject);
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
				if (handler.has("mod") && handler.get("mod").equals("riak_search_kv_hook")) return;
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
	
	@AfterClass
	public static void teardown() {
		RiakClient c = new RiakClient(RIAK_URL);
		for (int i = 0; i < TEST_ITEMS; i++) {
			c.delete(BUCKET_NAME, "java_" + Integer.toString(i));
			c.delete(SEARCH_BUCKET_NAME, "java_" + Integer.toString(i));
		}
	}
	
	@Test
	public void doSearchMapOnly() throws HttpException, IOException, JSONException {
		RiakClient c = new RiakClient(RIAK_URL);
		MapReduceResponse response = c.mapReduceOverSearch(SEARCH_BUCKET_NAME, "foo:zero").map(JavascriptFunction.anon("function(v) { return [v]; }"), true).submit();
		assertTrue(response.isSuccess());
		JSONArray results = response.getResults();
		assertEquals(TEST_ITEMS / 10, results.length());
	}
	
	@Test
	public void doLinkMapReduce() throws HttpException, IOException, JSONException {
		RiakClient c = new RiakClient(RIAK_URL);
		MapReduceResponse response = c.mapReduceOverBucket(BUCKET_NAME).link(BUCKET_NAME, "test", false)
		        .map(JavascriptFunction.named("Riak.mapValuesJson"), false).reduce(new ErlangFunction("riak_kv_mapreduce", "reduce_sort"), true).submit();
		assertTrue(response.isSuccess());
		JSONArray results = response.getResults();
		assertEquals(TEST_ITEMS - 1, results.length());
	}
	
	@Test
	public void doErlangMapReduce() throws HttpException, IOException, JSONException {
		RiakClient c = new RiakClient(RIAK_URL);
		MapReduceBuilder builder = new MapReduceBuilder(c);
		builder.setBucket(BUCKET_NAME);
		builder.map(new ErlangFunction("riak_kv_mapreduce", "map_object_value"), false);
		builder.reduce(new ErlangFunction("riak_kv_mapreduce", "reduce_string_to_integer"), false);
		builder.reduce(new ErlangFunction("riak_kv_mapreduce", "reduce_sort"), true);
		MapReduceResponse response = builder.submit();
		assertTrue(response.isSuccess());
		JSONArray results = response.getResults();
		assertEquals(TEST_ITEMS, results.length());
		assertEquals(0, results.getInt(0));
		assertEquals(73, results.getInt(73));
		assertEquals(197, results.getInt(197));
	}
	
	@Test
	public void doJavascriptMapReduce() throws HttpException, IOException, JSONException {
		RiakClient c = new RiakClient(RIAK_URL);
		MapReduceBuilder builder = new MapReduceBuilder(c);
		builder.setBucket(BUCKET_NAME);
		builder.map(JavascriptFunction.named("Riak.mapValuesJson"), false);
		builder.reduce(JavascriptFunction.named("Riak.reduceNumericSort"), true);
		MapReduceResponse response = builder.submit();
		assertTrue(response.isSuccess());
		JSONArray results = response.getResults();
		assertEquals(TEST_ITEMS, results.length());
		assertEquals(0, results.getInt(0));
		assertEquals(73, results.getInt(73));
		assertEquals(197, results.getInt(197));
	}
}
