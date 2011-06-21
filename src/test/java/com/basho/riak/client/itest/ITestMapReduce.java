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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.bucket.DomainBucket;
import com.basho.riak.client.bucket.RiakBucket;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.http.Hosts;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.filter.LessThanFilter;
import com.basho.riak.client.query.filter.StringToIntFilter;
import com.basho.riak.client.query.filter.TokenizeFilter;
import com.basho.riak.client.query.functions.JSSourceFunction;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import com.basho.riak.client.query.functions.NamedJSFunction;
import com.basho.riak.client.raw.query.MapReduceTimeoutException;
import com.megacorp.commerce.GoogleStockDataItem;

/**
 * @author russell
 * 
 */
public abstract class ITestMapReduce {
    protected IRiakClient client;

    @Before public void setUp() throws RiakException {
        client = getClient();
    }

    /**
     * @return
     * @throws RiakException
     */
    protected abstract IRiakClient getClient() throws RiakException;

    public static final String BUCKET_NAME = "mr_test_java";
    public static final int TEST_ITEMS = 200;

    @BeforeClass public static void setup() throws RiakException {
        final IRiakClient client = RiakFactory.pbcClient(Hosts.RIAK_HOST, Hosts.RIAK_PORT);
        final Bucket bucket = client.createBucket(BUCKET_NAME).execute();
        final RiakBucket b = RiakBucket.newRiakBucket(bucket);

        for (int i = 0; i < TEST_ITEMS; i++) {
            RiakObjectBuilder builder = RiakObjectBuilder.newBuilder(BUCKET_NAME, "java_" + Integer.toString(i));
            builder.withContentType("text/plain").withValue(Integer.toString(i));
            if (i < TEST_ITEMS - 1) {
                RiakLink link = new RiakLink(BUCKET_NAME, "java_" + Integer.toString(i + 1), "test");
                List<RiakLink> links = new ArrayList<RiakLink>(1);
                links.add(link);
                builder.withLinks(links);
            }

            b.store(builder.build());
        }
    }

    @AfterClass public static void teardown() throws RiakException {
        final IRiakClient client = RiakFactory.pbcClient(Hosts.RIAK_HOST, Hosts.RIAK_PORT);
        final Bucket b = client.fetchBucket(BUCKET_NAME).execute();

        for (int i = 0; i < TEST_ITEMS; i++) {
            b.delete("java_" + Integer.toString(i)).execute();
        }
    }

    @Test public void doLinkMapReduce() throws RiakException {
        MapReduceResult result = client.mapReduce(BUCKET_NAME)
        .addLinkPhase(BUCKET_NAME, "test")
        .addMapPhase(new NamedJSFunction("Riak.mapValuesJson"))
        .addReducePhase(new NamedErlangFunction("riak_kv_mapreduce", "reduce_sort"))
        .execute();

        assertNotNull(result);
        Collection<Integer> items = result.getResult(Integer.class);
        assertEquals(TEST_ITEMS - 1, items.size());
    }

    @Test public void doErlangMapReduce() throws RiakException {
        MapReduceResult result = client.mapReduce(BUCKET_NAME)
        .addMapPhase(new NamedErlangFunction("riak_kv_mapreduce","map_object_value"))
        .addReducePhase(new NamedErlangFunction("riak_kv_mapreduce","reduce_string_to_integer"))
        .addReducePhase(new NamedErlangFunction("riak_kv_mapreduce","reduce_sort"))
        .execute();

        assertNotNull(result);
        List<Integer> items = new LinkedList<Integer>(result.getResult(Integer.class));
        assertEquals(TEST_ITEMS, items.size());
        assertEquals(new Integer(0), items.get(0));
        assertEquals(new Integer(73), items.get(73));
        assertEquals(new Integer(197), items.get(197));
    }

    @Test public void doJavascriptMapReduce() throws RiakException {
        MapReduceResult result = client.mapReduce(BUCKET_NAME)
            .addMapPhase(new NamedJSFunction("Riak.mapValuesJson"))
            .addReducePhase(new NamedJSFunction("Riak.reduceNumericSort"))
            .execute();

        assertNotNull(result);
        List<Integer> items = new LinkedList<Integer>(result.getResult(Integer.class));
        assertEquals(TEST_ITEMS, items.size());
        assertEquals(new Integer(0), items.get(0));
        assertEquals(new Integer(73), items.get(73));
        assertEquals(new Integer(197), items.get(197));
    }

	@Test public void ignoreLastPhaseMapReduce() throws RiakException {
		MapReduceResult result = client
				.mapReduce(BUCKET_NAME)
				.addMapPhase(new NamedJSFunction("Riak.mapValuesJson"))
				.addReducePhase(new NamedJSFunction("Riak.reduceNumericSort"),
						false).execute();

		assertNotNull(result);
		List<Integer> items = new LinkedList<Integer>(
				result.getResult(Integer.class));
		assertEquals(0, items.size());
	}

    @Test public void doKeyFilterMapReduce() throws RiakException {
        MapReduceResult result = client.mapReduce(BUCKET_NAME)
        .addKeyFilter(new TokenizeFilter("_", 2))
        .addKeyFilter(new StringToIntFilter())
        .addKeyFilter(new LessThanFilter(50))
        .addMapPhase(new NamedJSFunction("Riak.mapValuesJson"))
        .addReducePhase(new NamedErlangFunction("riak_kv_mapreduce","reduce_sort"))
        .execute();
        
        assertNotNull(result);
        List<Integer> items = new LinkedList<Integer>(result.getResult(Integer.class));
        assertEquals(50, items.size());
        assertEquals(new Integer(0), items.get(0));
        assertEquals(new Integer(23), items.get(23));
        assertEquals(new Integer(49), items.get(49));
    }
    
    @Test public void mapResultToDomainObject() throws IOException, RiakException {
        // set up data
        final String json = "[{\"Date\":\"2010-01-04\",\"Open\":626.95,\"High\":629.51,\"Low\":624.24,\"Close\":626.75,\"Volume\":1956200,\"Adj. Close\":626.75}," +
        		"{\"Date\":\"2010-01-05\",\"Open\":627.18,\"High\":627.84,\"Low\":621.54,\"Close\":623.99,\"Volume\":3004700,\"Adj. Close\":623.99}," +
        		"{\"Date\":\"2010-01-06\",\"Open\":625.86,\"High\":625.86,\"Low\":606.36,\"Close\":608.26,\"Volume\":3978700,\"Adj. Close\":608.26}," +
        		"{\"Date\":\"2010-01-07\",\"Open\":609.4,\"High\":610,\"Low\":592.65,\"Close\":594.1,\"Volume\":6414300,\"Adj. Close\":594.1}," +
        		"{\"Date\":\"2010-01-08\",\"Open\":592,\"High\":603.25,\"Low\":589.11,\"Close\":602.02,\"Volume\":4724300,\"Adj. Close\":602.02}]";
        
        final LinkedList<GoogleStockDataItem> expected = new ObjectMapper()
                                    .readValue(json, 
                                               TypeFactory.collectionType(LinkedList.class, GoogleStockDataItem.class));
        
        final Bucket b = client.createBucket("goog").execute();
        final DomainBucket<GoogleStockDataItem> bucket = DomainBucket.builder(b, GoogleStockDataItem.class).build();
        
        for(GoogleStockDataItem i : expected) {
            bucket.store(i);
        }
        
        // perform test
        MapReduceResult result = client.mapReduce()
        .addInput("goog","2010-01-04")
        .addInput("goog","2010-01-05")
        .addInput("goog","2010-01-06")
        .addInput("goog","2010-01-07")
        .addInput("goog","2010-01-08")
        .addMapPhase(new NamedJSFunction("Riak.mapValuesJson"))
        .execute();
        
        LinkedList<GoogleStockDataItem> actual = new LinkedList<GoogleStockDataItem>( result.getResult(GoogleStockDataItem.class) );
        assertNotNull(actual);
        assertEquals(expected.size(), actual.size());
        
        assertTrue(expected.containsAll(actual));
        
        //teardown
        for(String k : b.keys()) {
            bucket.delete(k);
        }
    }

    @SuppressWarnings("rawtypes") @Test public void zeroResultsEmptyCollection() throws Exception {
        final String bucketName = UUID.randomUUID().toString();
        // perform test
        MapReduceResult result = client.mapReduce(bucketName)
        .addMapPhase(new NamedJSFunction("Riak.mapValuesJson"), true)
        .execute();

        LinkedList<Map> actual = new LinkedList<Map>( result.getResult(Map.class) );
        assertNotNull(actual);
        assertEquals(0, actual.size());
    }

    @Test public void resultCanBeCalledManyTimes() throws RiakException {
        MapReduceResult result = client.mapReduce(BUCKET_NAME)
        .addKeyFilter(new TokenizeFilter("_", 2))
        .addKeyFilter(new StringToIntFilter())
        .addKeyFilter(new LessThanFilter(50))
        .addMapPhase(new NamedJSFunction("Riak.mapValuesJson"))
        .addReducePhase(new NamedErlangFunction("riak_kv_mapreduce","reduce_sort"))
        .execute();

        assertNotNull(result);
        List<Integer> items = new LinkedList<Integer>(result.getResult(Integer.class));
        assertEquals(50, items.size());
        assertEquals(new Integer(0), items.get(0));
        assertEquals(new Integer(23), items.get(23));
        assertEquals(new Integer(49), items.get(49));

        List<Integer> items2 = new LinkedList<Integer>(result.getResult(Integer.class));
        assertEquals(50, items.size());
        assertEquals(new Integer(0), items2.get(0));
        assertEquals(new Integer(23), items2.get(23));
        assertEquals(new Integer(49), items2.get(49));
    }

    @Test public void timeoutMapReduceThrowsTimeoutException() throws Exception {
        // set up data
        final String json = "[{\"Date\":\"2010-01-04\",\"Open\":626.95,\"High\":629.51,\"Low\":624.24,\"Close\":626.75,\"Volume\":1956200,\"Adj. Close\":626.75}," +
                "{\"Date\":\"2010-01-05\",\"Open\":627.18,\"High\":627.84,\"Low\":621.54,\"Close\":623.99,\"Volume\":3004700,\"Adj. Close\":623.99}," +
                "{\"Date\":\"2010-01-06\",\"Open\":625.86,\"High\":625.86,\"Low\":606.36,\"Close\":608.26,\"Volume\":3978700,\"Adj. Close\":608.26}," +
                "{\"Date\":\"2010-01-07\",\"Open\":609.4,\"High\":610,\"Low\":592.65,\"Close\":594.1,\"Volume\":6414300,\"Adj. Close\":594.1}," +
                "{\"Date\":\"2010-01-08\",\"Open\":592,\"High\":603.25,\"Low\":589.11,\"Close\":602.02,\"Volume\":4724300,\"Adj. Close\":602.02}]";

        final LinkedList<GoogleStockDataItem> expected = new ObjectMapper()
                                    .readValue(json,
                                               TypeFactory.collectionType(LinkedList.class, GoogleStockDataItem.class));

        final Bucket b = client.createBucket("goog").execute();
        final DomainBucket<GoogleStockDataItem> bucket = DomainBucket.builder(b, GoogleStockDataItem.class).build();

        for(GoogleStockDataItem i : expected) {
            bucket.store(i);
        }

        try {
            client.mapReduce("goog").addMapPhase(new JSSourceFunction(sleepJs())).execute();
            fail("expected MapReduceTimeoutException");
        } catch (MapReduceTimeoutException e) {
            // NO-OP
        }

        // teardown
        for (String k : b.keys()) {
            bucket.delete(k);
        }
    }

    /**
     * A JS function that will cause a m/r timeout
     * @return String of js sleep function
     */
    private static final String sleepJs() {
        return "function(v) { " + "var millis=12000; var d=new Date();  var c=null; do { "
               + "c = new Date();  }  while(c-d < millis); }";
    }
}
