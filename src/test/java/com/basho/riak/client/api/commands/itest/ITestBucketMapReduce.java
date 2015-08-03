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
import com.basho.riak.client.core.RiakResultStreamListener;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.api.commands.buckets.StoreBucketProperties;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.mapreduce.BucketMapReduce;
import com.basho.riak.client.api.commands.mapreduce.MapReduce;
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
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.After;
import org.junit.Test;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Assume;
import org.junit.Before;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestBucketMapReduce extends ITestBase
{
    private final static RiakClient client = new RiakClient(cluster);
    private final static String mrBucketName = bucketName.toString() + "_mr";
    
    @Before
    public void changeBucketProps() throws ExecutionException, InterruptedException
    {
        if (testBucketType)
        {
            Namespace ns = new Namespace(bucketType.toString(), mrBucketName);
            StoreBucketProperties op = new StoreBucketProperties.Builder(ns).withAllowMulti(false).build();
            client.execute(op);
        }
    }
    
    @After
    public void cleanUp() throws InterruptedException, ExecutionException
    {
        // Because some of these tests blow up due to Riak bugs we need 
        // to clean up the mess here
        Namespace ns = new Namespace(mrBucketName);
        resetAndEmptyBucket(ns);
        
        if (testBucketType)
        {
            ns = new Namespace(bucketType.toString(), mrBucketName);
            resetAndEmptyBucket(ns);
        }
    }
    
    private void initValues(String bucketType) throws InterruptedException
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
        erlangBucketMR(bucketType.toString());
    }
    
    private void erlangBucketMR(String bucketType) throws InterruptedException, ExecutionException
    {
        initValues(bucketType);
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
        
        resetAndEmptyBucket(ns);
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
        JsBucketMR(bucketType.toString());
    }
    
    private void JsBucketMR(String bucketType) throws InterruptedException, ExecutionException
    {
        initValues(bucketType);
        
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

        resetAndEmptyBucket(ns);
    }
    
    @Test
    public void multiPhaseResult() throws InterruptedException, ExecutionException
    {
        initValues(Namespace.DEFAULT_BUCKET_TYPE);
        
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
        
        resetAndEmptyBucket(ns);
    }

    @Test
    public void multiPhaseResultStream() throws InterruptedException, ExecutionException
    {
        initValues(Namespace.DEFAULT_BUCKET_TYPE);

        final LinkedBlockingQueue<ArrayNode> results = new LinkedBlockingQueue<ArrayNode>();

        RiakResultStreamListener<MapReduce.Response> listener = new RiakResultStreamListener<MapReduce.Response>(){
            @Override
            public void handle(MapReduce.Response response) {
                results.add(response.getResultsFromAllPhases());
            }
        };

        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, mrBucketName);
        BucketMapReduce bmr =
                new BucketMapReduce.Builder()
                        .withNamespace(ns)
                        .withMapPhase(Function.newNamedJsFunction("Riak.mapValuesJson"), true)
                        .withReducePhase(Function.newNamedJsFunction("Riak.reduceNumericSort"), true)
                        .withResultStreamListener(listener)
                        .build();

        RiakFuture<MapReduce.Response, BinaryValue> future = client.executeAsync(bmr);

        future.await();
        assertTrue(future.isSuccess());

        MapReduce.Response response = future.get();

        // The query should return two phase results, each a JSON array containing
        // all the values, 0 - 199
        assertEquals(results.size(), response.getResultsFromAllPhases().size());

        //assertEquals(42, results.element().asInt());
        //assertEquals(199, results.contains(42));

        resetAndEmptyBucket(ns);
    }

    @Test
    public void keyFilter() throws InterruptedException, ExecutionException
    {
        initValues(Namespace.DEFAULT_BUCKET_TYPE);
        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, mrBucketName);
        BucketMapReduce bmr = 
            new BucketMapReduce.Builder()
                .withNamespace(ns)
                .withMapPhase(Function.newNamedJsFunction("Riak.mapValuesJson"))
                .withReducePhase(Function.newErlangFunction("riak_kv_mapreduce", "reduce_sort"),true)
                .withKeyFilter(new TokenizeFilter("_",3))
                .withKeyFilter(new StringToIntFilter())
                .withKeyFilter(new LogicalAndFilter(new LessThanFilter<Integer>(50), new GreaterThanFilter<Integer>(45)))
                .build();
        
        RiakFuture<MapReduce.Response, BinaryValue> future = client.executeAsync(bmr);
        
        future.await();
        assertTrue(future.isSuccess());
        
        MapReduce.Response response = future.get();
        assertEquals(4, response.getResultsFromAllPhases().size());
        assertEquals(46, response.getResultsFromAllPhases().get(0).asInt());
        assertEquals(49, response.getResultsFromAllPhases().get(3).asInt());
        
        resetAndEmptyBucket(ns);
    }
    
    @Test
    public void differentBucketType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testBucketType);
        
        initValues(bucketType.toString());
        Namespace ns = new Namespace(bucketType.toString(), mrBucketName);
        BucketMapReduce bmr = 
            new BucketMapReduce.Builder()
                .withNamespace(ns)
                .withMapPhase(Function.newAnonymousJsFunction(
                    "function(value, keydata, arg) {" +
                        "  var data = value.values[0].data;" +
                        "  if(data > 20)" +
                        "    return [data];" +
                        "  else" +
                        "    return[];" +
                        "}"), true)
                .build();
        
        MapReduce.Response response = client.execute(bmr);
        
        assertEquals(179, response.getResultsFromAllPhases().size());
        
        resetAndEmptyBucket(ns);
    }
    
    @Test
    public void differentBucketTypeWithFilter() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testBucketType);

        initValues(bucketType.toString());
        
         Namespace ns = new Namespace(bucketType.toString(), mrBucketName);
        BucketMapReduce bmr = 
            new BucketMapReduce.Builder()
                .withNamespace(ns)
                .withKeyFilter(new TokenizeFilter("_",3))
                .withKeyFilter(new StringToIntFilter())
                .withKeyFilter(new LogicalAndFilter(new LessThanFilter<Integer>(50), new GreaterThanFilter<Integer>(45)))
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
        resetAndEmptyBucket(ns);
    }
}
