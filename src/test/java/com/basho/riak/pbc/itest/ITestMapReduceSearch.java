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

import static com.basho.riak.client.http.Hosts.RIAK_HOST;
import static com.basho.riak.client.http.Hosts.RIAK_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.json.JSONArray;
import org.json.JSONException;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.basho.riak.client.RiakTestProperties;
import com.basho.riak.client.http.itest.ITestMapReduceSearch.Digit;
import com.basho.riak.pbc.MapReduceResponseSource;
import com.basho.riak.pbc.RiakClient;
import com.basho.riak.pbc.RiakObject;
import com.basho.riak.pbc.mapreduce.JavascriptFunction;
import com.basho.riak.pbc.mapreduce.MapReduceBuilder;

/**
 * Exercises search map/reduce features of the Riak client. Assumes Riak is reachable
 * at {@link com.basho.riak.client.Hosts#RIAK_HOST } and
 * {@link com.basho.riak.client.Hosts#RIAK_PORT }
 * 
 * @see com.basho.riak.client.Hosts#RIAK_HOST
 * @see com.basho.riak.client.Hosts#RIAK_PORT
 */
public class ITestMapReduceSearch {

    public static String BUCKET = "pbc_mr_test_java";
	public static String	SEARCH_BUCKET_NAME	= "pbc_mr_stest_java";
    public static int TEST_ITEMS = 200;

    @BeforeClass public static void setup() throws Exception {
        Assume.assumeTrue(RiakTestProperties.isSearchEnabled());
        final RiakClient c = new RiakClient(RIAK_HOST, RIAK_PORT);
        for (int i = 0; i < TEST_ITEMS; i++) {
			RiakObject searchObject = new RiakObject(SEARCH_BUCKET_NAME, "java_" + Integer.toString(i), "{\"foo\":\"" + Digit.values()[i % 10].toString().toLowerCase() + "\"}");
			searchObject.setContentType("application/json");
			c.store(searchObject);
        }
    }

    @AfterClass public static void teardown() throws Exception {
        if (RiakTestProperties.isSearchEnabled()) {
            final RiakClient c = new RiakClient(RIAK_HOST, RIAK_PORT);

            for (int i = 0; i < TEST_ITEMS; i++) {
                c.delete(SEARCH_BUCKET_NAME, "java_" + Integer.toString(i));
            }
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
	public void doSearchMapOnly() throws IOException, JSONException {
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
}