/*
 * Copyright 2014 Brian Roach <roach at basho dot com>.
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
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.api.commands.StoreBucketProperties;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.mapreduce.IndexMapReduce;
import com.basho.riak.client.api.commands.mapreduce.MapReduce;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.functions.Function;
import com.basho.riak.client.core.query.indexes.LongIntIndex;
import com.basho.riak.client.core.util.BinaryValue;
import java.util.concurrent.ExecutionException;
import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestIndexMapReduce extends ITestBase
{
    private final RiakClient client = new RiakClient(cluster);
    private final  String mrBucketName = bucketName.toString() + "_mr";
    
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
    
    private void initValues(String bucketType) throws InterruptedException
    {
        String keyPrefix = "mr_test_";
        Namespace ns = new Namespace(bucketType, mrBucketName);
        for (int i = 0; i < 200; i++)
        {
            Location loc = new Location(ns, keyPrefix + i);
            RiakObject ro = new RiakObject().setContentType("text/plain")
                .setValue(BinaryValue.create(Integer.toString(i)));
            ro.getIndexes().getIndex(LongIntIndex.named("user_id")).add((long)i);
            StoreValue sv = new StoreValue.Builder(ro).withLocation(loc).build();
            RiakFuture<StoreValue.Response, Location> future = client.executeAsync(sv);
            future.await();
            assertTrue(future.isSuccess());
        }
    }
    
    private void initValuesOneToN(String bucketType) throws InterruptedException
    {
        String keyPrefix = "mr_test_";
        Namespace ns = new Namespace(bucketType, mrBucketName);
        for (int i = 0; i < 200; i++)
        {
            Location loc = new Location(ns, keyPrefix + i);
            RiakObject ro = new RiakObject().setContentType("text/plain")
                .setValue(BinaryValue.create(Integer.toString(i)));
            // Single index key used on all 200 objects
            ro.getIndexes().getIndex(LongIntIndex.named("user_id")).add(1L);
            StoreValue sv = new StoreValue.Builder(ro).withLocation(loc).build();
            RiakFuture<StoreValue.Response, Location> future = client.executeAsync(sv);
            future.await();
            assertTrue(future.isSuccess());
        }
    }
    
    @Test
    public void matchIndex() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        initValuesOneToN(Namespace.DEFAULT_BUCKET_TYPE);
        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, mrBucketName);
        IndexMapReduce imr = new IndexMapReduce.Builder()
                            .withNamespace(ns)
                            .withIndex("user_id_int")
                            .withMatchValue(1L)
                            .withMapPhase(Function.newAnonymousJsFunction(
                                "function(value, keydata, arg) {" +
                                "  var data = value.values[0].data;" +
                                "  return [data];" +
                                "}"), true)
                            .build();
        
        MapReduce.Response response = client.execute(imr);
        
        assertEquals(200, response.getResultsFromAllPhases().size());
                
        resetAndEmptyBucket(ns);

    }
    
    @Test
    public void matchIndexDiffType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testBucketType);
        Assume.assumeTrue(test2i);
        
        initValuesOneToN(bucketType.toString());
        Namespace ns = new Namespace(bucketType.toString(), mrBucketName);
        IndexMapReduce imr = new IndexMapReduce.Builder()
                            .withNamespace(ns)
                            .withIndex("user_id_int")
                            .withMatchValue(1L)
                            .withMapPhase(Function.newAnonymousJsFunction(
                                "function(value, keydata, arg) {" +
                                "  var data = value.values[0].data;" +
                                "  return [data];" +
                                "}"), true)
                            .build();
        
        MapReduce.Response response = client.execute(imr);
        
        assertEquals(200, response.getResultsFromAllPhases().size());
                
        resetAndEmptyBucket(ns);
    }
    
    @Test
    public void rangeIndex() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(test2i);
        initValues(Namespace.DEFAULT_BUCKET_TYPE);
        
        Namespace ns = new Namespace(mrBucketName);
        IndexMapReduce imr = new IndexMapReduce.Builder()
                            .withNamespace(ns)
                            .withIndex("user_id_int")
                            .withRange(0, 19)
                            .withMapPhase(Function.newAnonymousJsFunction(
                                "function(value, keydata, arg) {" +
                                "  var data = value.values[0].data;" +
                                "  return [data];" +
                                "}"), true)
                            .build();
        
         MapReduce.Response response = client.execute(imr);
        
        assertEquals(20, response.getResultsFromAllPhases().size());
                
        resetAndEmptyBucket(ns);
        
        
    }
    
    @Test
    public void rangeIndexDiffType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testBucketType);
        Assume.assumeTrue(test2i);
        
        initValues(bucketType.toString());
        Namespace ns = new Namespace(bucketType.toString(), mrBucketName);
        IndexMapReduce imr = new IndexMapReduce.Builder()
                            .withNamespace(ns)
                            .withIndex("user_id_int")
                            .withRange(0, 19)
                            .withMapPhase(Function.newAnonymousJsFunction(
                                "function(value, keydata, arg) {" +
                                "  var data = value.values[0].data;" +
                                "  return [data];" +
                                "}"), true)
                            .build();
        
         MapReduce.Response response = client.execute(imr);
        
        assertEquals(20, response.getResultsFromAllPhases().size());
                
        resetAndEmptyBucket(ns);
        
        
    }
}
