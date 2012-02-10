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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Collection;
import java.util.Map;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakTestProperties;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.bucket.RiakBucket;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.http.itest.ITestMapReduceSearch.Digit;
import com.basho.riak.client.http.util.Constants;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.functions.JSSourceFunction;

/**
 * @author russell
 * 
 */
public abstract class ITestMapReduceSearch {

    protected IRiakClient client;
    private RiakBucket bucket;

    /**
     * @return the riak client to use
     * @throws RiakException
     */
    protected abstract IRiakClient getClient() throws RiakException;

    public static String SEARCH_BUCKET_NAME = "mr_test_search";
    public static int TEST_ITEMS = 200;

    @Before public  void setup() throws RiakException {
        Assume.assumeTrue(RiakTestProperties.isSearchEnabled());
        client = getClient();
        final Bucket b = indexBucket(client, SEARCH_BUCKET_NAME);
        bucket = RiakBucket.newRiakBucket(b);

        for (int i = 0; i < TEST_ITEMS; i++) {
            RiakObjectBuilder builder = RiakObjectBuilder.newBuilder(SEARCH_BUCKET_NAME,  "java_" + Integer.toString(i))
                .withContentType(Constants.CTYPE_JSON)
                .withValue("{\"foo\":\"" + Digit.values()[i % 10].toString().toLowerCase() + "\"}");

            bucket.store(builder.build());
        }
    }

    private Bucket indexBucket(IRiakClient c, String bucket) throws RiakException {
        return c.createBucket(SEARCH_BUCKET_NAME).enableForSearch()
                .execute();
    }

    @After public void teardown() throws RiakException {
        if (bucket != null) {
            for (int i = 0; i < TEST_ITEMS; i++) {
                bucket.delete("java_" + Integer.toString(i));
            }
        }
    }

    @Test public void doSearchMapOnly() throws RiakException {
        MapReduceResult result =  client.mapReduce(SEARCH_BUCKET_NAME, "foo:zero")
           .addMapPhase(new JSSourceFunction("function(v) { return [v]; }"), true).execute();

        assertNotNull(result);
        @SuppressWarnings("rawtypes") Collection<Map> items = result.getResult(Map.class);
        assertEquals(TEST_ITEMS / 10, items.size());
    }
}
