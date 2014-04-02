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

package com.basho.riak.client.operations.itest;

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.operations.kv.StoreValue;
import com.basho.riak.client.operations.mapreduce.BucketMapReduce;
import com.basho.riak.client.operations.mapreduce.MapReduce;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.query.filters.GreaterThanFilter;
import com.basho.riak.client.query.filters.LessThanFilter;
import com.basho.riak.client.query.filters.LogicalAndFilter;
import com.basho.riak.client.query.filters.StringToIntFilter;
import com.basho.riak.client.query.filters.TokenizeFilter;
import com.basho.riak.client.query.functions.Function;
import com.basho.riak.client.util.BinaryValue;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.concurrent.ExecutionException;
import org.junit.Test;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Assume;
import org.junit.Ignore;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestBucketMapReduce extends ITestBase
{
    private final RiakClient client = new RiakClient(cluster);
    private final  String mrBucketName = bucketName.toString() + "_mr";
    
    private void initValues(String bucketType) throws InterruptedException
    {
        // insert 200 items into a bucket
       
        String keyPrefix = "mr_test_";
        for (int i = 0; i < 200; i++)
        {
            Location loc = new Location(mrBucketName).setBucketType(bucketType).setKey(keyPrefix + i);
            RiakObject ro = new RiakObject().setContentType("text/plain")
                .setValue(BinaryValue.create(Integer.toString(i)));
            StoreValue sv = new StoreValue.Builder(ro).withLocation(loc).build();
            RiakFuture<StoreValue.Response, Location> future = client.executeAsync(sv);
            future.await();
            assertTrue(future.isSuccess());
        }
    }
    
    @Test
    public void erlangBucketMR() throws InterruptedException, ExecutionException
    {
        initValues("default");
        
        Location loc = new Location(mrBucketName);
        BucketMapReduce bmr = 
            new BucketMapReduce.Builder()
                .withBucket(loc)
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
        
        resetAndEmptyBucket(loc);
    }
    
    @Test
    public void JsBucketMR() throws InterruptedException, ExecutionException
    {
        initValues("default");
        
        Location loc = new Location(mrBucketName);
        BucketMapReduce bmr = 
            new BucketMapReduce.Builder()
                .withBucket(loc)
                .withMapPhase(Function.newNamedJsFunction("Riak.mapValuesJson"), false)
                .withReducePhase(Function.newNamedJsFunction("Riak.reduceNumericSort"), true)
                .build();
        
        RiakFuture<MapReduce.Response, BinaryValue> future = client.executeAsync(bmr);
        
        future.await();
        assertTrue(future.isSuccess());
        
        MapReduce.Response response = future.get();
        
        // The query should return one phase result which is a JSON array containing
        // all the values, 0 - 199
        assertEquals(200, response.getResultsFromAllPhases().size());
        ArrayNode result = response.getResultForPhase(1);
        assertEquals(200, result.size());
        
        assertEquals(42, result.get(42).asInt());
        assertEquals(199, result.get(199).asInt());
               
        resetAndEmptyBucket(loc);
    }
    
    @Test
    public void multiPhaseResult() throws InterruptedException, ExecutionException
    {
        initValues("default");
        
        Location loc = new Location(mrBucketName);
        BucketMapReduce bmr = 
            new BucketMapReduce.Builder()
                .withBucket(loc)
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
        
        resetAndEmptyBucket(loc);
    }
    
    @Test
    public void keyFilter() throws InterruptedException, ExecutionException
    {
        initValues("default");
        Location loc = new Location(mrBucketName);
        BucketMapReduce bmr = 
            new BucketMapReduce.Builder()
                .withBucket(loc)
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
        
        resetAndEmptyBucket(loc);
    }
    
    @Test
    public void differentBucketType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testBucketType);
        
        initValues(bucketType.toString());
        
        Location loc = new Location(mrBucketName).setBucketType(bucketType);
        BucketMapReduce bmr = 
            new BucketMapReduce.Builder()
                .withBucket(loc)
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
        
        resetAndEmptyBucket(loc);
    }
    
    // This is yet another bug in Riak and will not work. Engel is supposed to 
    // fix it in Riak.
    @Ignore
    @Test
    public void differentBucketTypeWithFilter() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testBucketType);

        initValues(bucketType.toString());
        
        Location loc = new Location(mrBucketName).setBucketType(bucketType);
        BucketMapReduce bmr = 
            new BucketMapReduce.Builder()
                .withBucket(loc)
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
        resetAndEmptyBucket(loc);

    }
    
}
