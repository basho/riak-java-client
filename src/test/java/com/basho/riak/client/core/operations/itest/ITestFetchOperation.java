/*
 * Copyright 2013 Basho Technologies Inc.
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
package com.basho.riak.client.core.operations.itest;

import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.FetchBucketPropsOperation;
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.core.operations.StoreBucketPropsOperation;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestFetchOperation extends ITestBase
{
    private static long keySeed = new Random().nextLong();
    private static String siblingsBucket = "siblings";

    @BeforeClass
    public static void setupSiblingBuckets() throws ExecutionException, InterruptedException
    {
        setAllowMultOnBucket(new Namespace(Namespace.DEFAULT_BUCKET_TYPE, siblingsBucket));
        setAllowMultOnBucket(new Namespace(bucketType.toStringUtf8(), siblingsBucket));
    }

    @AfterClass
    public static void cleanupBuckets() throws ExecutionException, InterruptedException
    {
        ITestAutoCleanupBase.resetAndEmptyBucket(new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString()));
        ITestAutoCleanupBase.resetAndEmptyBucket(new Namespace(Namespace.DEFAULT_BUCKET_TYPE, siblingsBucket));
        ITestAutoCleanupBase.resetAndEmptyBucket(new Namespace(bucketType.toString(), bucketName.toString()));
        ITestAutoCleanupBase.resetAndEmptyBucket(new Namespace(bucketType.toString(), siblingsBucket));
    }

    @Test
    public void testFetchOpNotFoundDefaultType() throws InterruptedException, ExecutionException
    {
        testFetchOpNotFound(Namespace.DEFAULT_BUCKET_TYPE);
    }
    
    @Test
    public void testFetchOpNotFoundTestType() throws InterruptedException, ExecutionException
    {
        assumeTrue(testBucketType);
        testFetchOpNotFound(bucketType.toString());
    }
    
    private void testFetchOpNotFound(String bucketType) throws InterruptedException, ExecutionException
    {
        final BinaryValue key = generateKey();

        Location location = new Location(new Namespace(bucketType, bucketName.toString()), key);

        FetchOperation fetchOp =
            new FetchOperation.Builder(location).build();
                
        cluster.execute(fetchOp);
        FetchOperation.Response response = fetchOp.get();
        assertTrue(response.isNotFound());
        assertTrue(response.getObjectList().isEmpty());
        
    }
    
    @Test
    public void testFetchOpNoSiblingsDefaultType() throws InterruptedException, ExecutionException
    {
        testFetchOpNoSiblings(Namespace.DEFAULT_BUCKET_TYPE);
    }
    
    @Test
    public void testFetchOpNoSiblingsTestType() throws InterruptedException, ExecutionException
    {
        assumeTrue(testBucketType);
        testFetchOpNoSiblings(bucketType.toString());
    }
    
    private void testFetchOpNoSiblings(String bucketType) throws InterruptedException, ExecutionException
    {
        final BinaryValue key = generateKey();
        final String value = "{\"value\":\"value\"}";
        
        RiakObject rObj = new RiakObject().setValue(BinaryValue.create(value));
        Namespace ns = new Namespace(bucketType, bucketName.toString());
        Location location = new Location(ns, key);
        StoreOperation storeOp = 
            new StoreOperation.Builder(location)
                .withContent(rObj)
                .build(); 
        
        cluster.execute(storeOp);
        storeOp.get();
        
        FetchOperation fetchOp = 
            new FetchOperation.Builder(location).build();
        
        cluster.execute(fetchOp);
        FetchOperation.Response response = fetchOp.get();
        assertFalse(response.isNotFound());
        List<RiakObject> objectList = response.getObjectList();
        assertEquals(1, objectList.size());
        RiakObject ro = objectList.get(0);
        assertFalse(ro.isDeleted());
        assertEquals(ro.getValue().toString(), value);
        
    }
    
    @Test

    public void testFetchOpWithSiblingsDefaultType() throws InterruptedException, ExecutionException
    {
        testFetchOpWithSiblings(Namespace.DEFAULT_BUCKET_TYPE);
    }
    
    @Test
    public void testFetchOpWithSiblingsTestType() throws InterruptedException, ExecutionException
    {
        assumeTrue(testBucketType);
        testFetchOpWithSiblings(bucketType.toString());
    }
    
    private void testFetchOpWithSiblings(String bucketType) throws InterruptedException, ExecutionException
    {
        final BinaryValue key = generateKey();
        final String value = "{\"value\":\"value\"}";

        Namespace namespace = new Namespace(bucketType, siblingsBucket.toString());

        RiakObject rObj = new RiakObject().setValue(BinaryValue.create(value));
        Location location = new Location(namespace, key);
        final StoreOperation storeOp = new StoreOperation.Builder(location).withContent(rObj).build();

        cluster.execute(storeOp);
        storeOp.get();
        assertTrue(storeOp.isSuccess());


        final StoreOperation storeOp2 = new StoreOperation.Builder(location).withContent(rObj).build();

        cluster.execute(storeOp2);
        storeOp2.get();
        assertTrue(storeOp2.isSuccess());

        FetchOperation fetchOp =
                new FetchOperation.Builder(location).build();
        System.out.println(key.toStringUtf8());
        cluster.execute(fetchOp);
        FetchOperation.Response response = fetchOp.get();

        assertTrue(response.getObjectList().size() > 1);
        RiakObject ro = response.getObjectList().get(0);
        assertEquals(ro.getValue().toString(), value);
    }

    private static Namespace setAllowMultOnBucket(Namespace namespace) throws InterruptedException, ExecutionException
    {
        StoreBucketPropsOperation op =
            new StoreBucketPropsOperation.Builder(namespace)
                .withAllowMulti(true)
                .build();
        final RiakFuture<Void, Namespace> storePropsFuture = cluster.execute(op);
        storePropsFuture.get();

        FetchBucketPropsOperation fetchPropsOp = new FetchBucketPropsOperation.Builder(namespace).build();
        final RiakFuture<FetchBucketPropsOperation.Response, Namespace> fetchFuture = cluster.execute(fetchPropsOp);
        final FetchBucketPropsOperation.Response response = fetchFuture.get();
        assertTrue(response.getBucketProperties().getAllowMulti());
        return namespace;
    }

    private BinaryValue generateKey()
    {
        final String key = testName.getMethodName() + keySeed;
        return BinaryValue.unsafeCreate(key.getBytes());
    }

}
