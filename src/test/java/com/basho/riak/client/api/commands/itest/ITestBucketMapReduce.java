/*
 * Copyright 2014 Basho Technologies Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.basho.riak.client.api.commands.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.api.commands.buckets.StoreBucketProperties;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.mapreduce.BucketMapReduce;
import com.basho.riak.client.api.commands.mapreduce.MapReduce;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.api.commands.mapreduce.filters.GreaterThanFilter;
import com.basho.riak.client.api.commands.mapreduce.filters.LessThanFilter;
import com.basho.riak.client.api.commands.mapreduce.filters.LogicalAndFilter;
import com.basho.riak.client.api.commands.mapreduce.filters.StringToIntFilter;
import com.basho.riak.client.api.commands.mapreduce.filters.TokenizeFilter;
import com.basho.riak.client.core.query.functions.Function;
import com.basho.riak.client.core.util.BinaryValue;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.concurrent.ExecutionException;

import org.junit.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestBucketMapReduce extends ITestBase
{
    private final static RiakClient client = new RiakClient(cluster);
    private final static String mrBucketName = "ITestBucketMapReduce";

    @BeforeClass
    public static void setup() throws InterruptedException, ExecutionException
    {
        changeBucketProps();
        initValues(Namespace.DEFAULT_BUCKET_TYPE.toString());
        if (testBucketType)
        {
            initValues(mapReduceBucketType.toString());
        }
    }

    @AfterClass
    public static void tearDown() throws ExecutionException, InterruptedException
    {
        Namespace ns = new Namespace(mrBucketName);
        resetAndEmptyBucket(ns);

        if (testBucketType)
        {
            ns = new Namespace(mapReduceBucketType.toString(), mrBucketName);
            resetAndEmptyBucket(ns);
        }
    }

    private static void changeBucketProps() throws ExecutionException, InterruptedException
    {
        if (testBucketType)
        {
            Namespace ns = new Namespace(mapReduceBucketType.toString(), mrBucketName);
            StoreBucketProperties op = new StoreBucketProperties.Builder(ns).withAllowMulti(false).build();
            client.execute(op);
        }
    }

    private static void initValues(String bucketType) throws InterruptedException
    {
        // insert 200 items into a bucket

        Namespace ns = new Namespace(bucketType, mrBucketName);

        String keyPrefix = "mr_test_";
        for (int i = 0; i < 200; i++)
        {
            Location loc = new Location(ns, keyPrefix + i);
            RiakObject ro = new RiakObject().setContentType("text/plain")
                .setValue(BinaryValue.create(Integer.toString(i)));
            StoreValue sv = new StoreValue.Builder(ro).withLocation(loc).build();
            RiakFuture<StoreValue.Response, Location> future = client.executeAsync(sv);
            future.await();
            assertTrue(future.isSuccess());
        }
    }

    @Test
    public void erlangBucketMRDefaultType() throws InterruptedException, ExecutionException
    {
        erlangBucketMR(Namespace.DEFAULT_BUCKET_TYPE);
    }

    @Test
    public void erlangBucketMRTestType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testBucketType);
        erlangBucketMR(mapReduceBucketType.toString());
    }

    @Test
    public void erlangBucketMRDefaultTypeStreaming() throws InterruptedException, ExecutionException
    {
        streamingErlangBucketMR(Namespace.DEFAULT_BUCKET_TYPE);
    }

    @Test
    public void erlangBucketMRTestTypeStreaming() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testBucketType);
        streamingErlangBucketMR(mapReduceBucketType.toString());
    }

    private void erlangBucketMR(String bucketType) throws InterruptedException, ExecutionException
    {
        Namespace ns = new Namespace(bucketType, mrBucketName);
        BucketMapReduce bmr =
            new BucketMapReduce.Builder()
                .withNamespace(ns)
                .withMapPhase(Function.newErlangFunction("riak_kv_mapreduce", "map_object_value"), false)
                .withReducePhase(Function.newErlangFunction("riak_kv_mapreduce", "reduce_string_to_integer"), false)
                .withReducePhase(Function.newErlangFunction("riak_kv_mapreduce", "reduce_sort"), true)
                .build();

         MapReduce.Response response = client.execute(bmr);

        // The query should return one phase result which is a JSON array containing
        // all the values, 0 - 199
        assertEquals(200, response.getResultsFromAllPhases().size());
        ArrayNode result = response.getResultForPhase(2);
        assertEquals(200, result.size());

        assertEquals(42, result.get(42).asInt());
        assertEquals(199, result.get(199).asInt());
    }

    private void streamingErlangBucketMR(String bucketType) throws InterruptedException, ExecutionException
    {
        Namespace ns = new Namespace(bucketType, mrBucketName);
        BucketMapReduce bmr =
                new BucketMapReduce.Builder()
                        .withNamespace(ns)
                        .withMapPhase(Function.newErlangFunction("riak_kv_mapreduce", "map_object_value"), false)
                        .withReducePhase(Function.newErlangFunction("riak_kv_mapreduce", "reduce_string_to_integer"), false)
                        .withReducePhase(Function.newErlangFunction("riak_kv_mapreduce", "reduce_sort"), true)
                        .build();

        final RiakFuture<MapReduce.Response, BinaryValue> streamingFuture =
                client.executeAsyncStreaming(bmr, 10);

        boolean found42 = false;
        boolean found199 = false;
        int count = 0;

        final MapReduce.Response streamingResponse = streamingFuture.get();
        assertTrue(streamingResponse.isStreaming());
        // The streaming query should return many results which are JSON arrays, each
        // containing a piece of the array [0-199].
        // Streaming result would look like: [[0], [1,2,3], ... [..., 199]], with the outer
        // array being the different response chunks streaming in.
        for (MapReduce.Response response : streamingResponse)
        {
            int phaseSize = response.getResultsFromAllPhases().size();

            if (phaseSize == 0)
            {
                continue;
            }

            count += phaseSize;

            final ArrayNode result = response.getResultForPhase(2);
            if (result == null)
            {
                continue;
            }

            final String valuesString = result.toString();

            if (!found42)
            {
                found42 = valuesString.contains("42");
            }
            if (!found199)
            {
                found199 = valuesString.contains("199");
            }
        }

        assertEquals(200, count);
        assertTrue(found42);
        assertTrue(found199);

        // Assert that we have consumed the responses, and none are left.
        assertFalse(streamingFuture.get().iterator().hasNext());
    }

    @Test
    public void JsBucketMRDefaultType() throws InterruptedException, ExecutionException
    {
        JsBucketMR(Namespace.DEFAULT_BUCKET_TYPE);
    }

    @Test
    public void JsBucketMRTestType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testBucketType);
        JsBucketMR(mapReduceBucketType.toString());
    }

    private void JsBucketMR(String bucketType) throws InterruptedException, ExecutionException
    {
        Namespace ns = new Namespace(bucketType, mrBucketName);
        BucketMapReduce bmr =
            new BucketMapReduce.Builder()
                .withNamespace(ns)
                .withMapPhase(Function.newNamedJsFunction("Riak.mapValuesJson"), false)
                .withReducePhase(Function.newNamedJsFunction("Riak.reduceNumericSort"), true)
                .build();

        RiakFuture<MapReduce.Response, BinaryValue> future = client.executeAsync(bmr);

        future.await();
        assertTrue("Map reduce operation Operation failed:" + future.cause(), future.isSuccess());

        MapReduce.Response response = future.get();

        // The query should return one phase result which is a JSON array containing
        // all the values, 0 - 199
        assertEquals(200, response.getResultsFromAllPhases().size());
        ArrayNode result = response.getResultForPhase(1);
        assertEquals(200, result.size());

        assertEquals(42, result.get(42).asInt());
        assertEquals(199, result.get(199).asInt());
    }

    @Test
    public void multiPhaseResult() throws InterruptedException, ExecutionException
    {
        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, mrBucketName);
        BucketMapReduce bmr =
            new BucketMapReduce.Builder()
                .withNamespace(ns)
                .withMapPhase(Function.newNamedJsFunction("Riak.mapValuesJson"), true)
                .withReducePhase(Function.newNamedJsFunction("Riak.reduceNumericSort"), true)
                .build();

        RiakFuture<MapReduce.Response, BinaryValue> future = client.executeAsync(bmr);

        future.await();
        assertTrue(future.isSuccess());

        MapReduce.Response response = future.get();

        // The query should return two phase results, each a JSON array containing
        // all the values, 0 - 199
        assertEquals(400, response.getResultsFromAllPhases().size());
        assertEquals(200, response.getResultForPhase(0).size());
        ArrayNode result = response.getResultForPhase(1);
        assertEquals(200, result.size());

        assertEquals(42, result.get(42).asInt());
        assertEquals(199, result.get(199).asInt());
    }

    @Test
    public void keyFilter() throws InterruptedException, ExecutionException
    {
        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, mrBucketName);
        BucketMapReduce bmr =
            new BucketMapReduce.Builder()
                .withNamespace(ns)
                .withMapPhase(Function.newNamedJsFunction("Riak.mapValuesJson"))
                .withReducePhase(Function.newErlangFunction("riak_kv_mapreduce", "reduce_sort"),true)
                .withKeyFilter(new TokenizeFilter("_",3))
                .withKeyFilter(new StringToIntFilter())
                .withKeyFilter(new LogicalAndFilter(new LessThanFilter<>(50), new GreaterThanFilter<>(45)))
                .build();

        RiakFuture<MapReduce.Response, BinaryValue> future = client.executeAsync(bmr);

        future.await();
        assertTrue(future.isSuccess());

        MapReduce.Response response = future.get();
        assertEquals(4, response.getResultsFromAllPhases().size());
        assertEquals(46, response.getResultsFromAllPhases().get(0).asInt());
        assertEquals(49, response.getResultsFromAllPhases().get(3).asInt());
    }

    @Test
    public void differentBucketType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testBucketType);

        Namespace ns = new Namespace(mapReduceBucketType.toString(), mrBucketName);
        BucketMapReduce bmr =
            new BucketMapReduce.Builder()
                .withNamespace(ns)
                .withMapPhase(Function.newAnonymousJsFunction(
                    "function(value, keydata, arg) {" +
                        "  var data = value.values[0].data;" +
                        "  if (data > 20)" +
                        "    return [data];" +
                        "  else" +
                        "    return[];" +
                        "}"), true)
                .build();

        MapReduce.Response response = client.execute(bmr);

        assertEquals(179, response.getResultsFromAllPhases().size());
    }

    @Test
    public void differentBucketTypeWithFilter() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testBucketType);

        Namespace ns = new Namespace(mapReduceBucketType.toString(), mrBucketName);
        BucketMapReduce bmr =
            new BucketMapReduce.Builder()
                .withNamespace(ns)
                .withKeyFilter(new TokenizeFilter("_",3))
                .withKeyFilter(new StringToIntFilter())
                .withKeyFilter(new LogicalAndFilter(new LessThanFilter<>(50), new GreaterThanFilter<>(45)))
                .withMapPhase(Function.newAnonymousJsFunction(
                    "function(value, keydata, arg) {" +
                        "  var data = value.values[0].data;" +
                        "  return [data];" +
                        "}"), true)
                .build();

        RiakFuture<MapReduce.Response, BinaryValue> future = client.executeAsync(bmr);

        future.await();
        assertTrue(future.isSuccess());
        assertEquals(4, future.get().getResultsFromAllPhases().size());
    }
}
