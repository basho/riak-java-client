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
import com.basho.riak.client.core.RiakFutureListener;
import com.basho.riak.client.core.operations.DeleteOperation;
import com.basho.riak.client.core.operations.ListKeysOperation;
import com.basho.riak.client.core.operations.ResetBucketPropsOperation;
import com.basho.riak.client.core.operations.YzFetchIndexOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.After;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class ITestAutoCleanupBase extends ITestBase
{
    @After
    public void afterTest() throws ExecutionException, InterruptedException
    {
        resetAndEmptyBucket(bucketName);
        if (testBucketType)
        {
            resetAndEmptyBucket(defaultNamespace());
        }
    }
    
    public static void resetAndEmptyBucket(BinaryValue name) throws InterruptedException, ExecutionException
    {
        resetAndEmptyBucket(new Namespace(Namespace.DEFAULT_BUCKET_TYPE, name.toString()));
    }

    public static void resetAndEmptyBucket(Namespace namespace) throws InterruptedException, ExecutionException
    {
        ListKeysOperation.Builder keysOpBuilder = new ListKeysOperation.Builder(namespace);
        
        ListKeysOperation keysOp = keysOpBuilder.build();
        cluster.execute(keysOp);
        List<BinaryValue> keyList = keysOp.get().getKeys();
        final Semaphore semaphore = new Semaphore(NUMBER_OF_PARALLEL_REQUESTS);
        final CountDownLatch latch = new CountDownLatch(keyList.size());
        
        RiakFutureListener<Void, Location> listener = new RiakFutureListener<Void, Location>() {
            @Override
            public void handle(RiakFuture<Void, Location> f)
            {
                try
                {
                    f.get();
                }
                catch (Exception ex)
                {
                    if (ex instanceof RuntimeException)
                    {
                        throw (RuntimeException)ex;
                    }
                    throw new RuntimeException(ex);
                }
                semaphore.release();
                latch.countDown();
            }
            
        };
        
        for (BinaryValue k : keyList)
        {
            Location location = new Location(namespace, k);
            DeleteOperation delOp = new DeleteOperation.Builder(location).withRw(3).build();
            delOp.addListener(listener);
            semaphore.acquire();
            cluster.execute(delOp);
        }

        latch.await();

        ResetBucketPropsOperation.Builder resetOpBuilder = 
            new ResetBucketPropsOperation.Builder(namespace);
        
        ResetBucketPropsOperation resetOp = resetOpBuilder.build();
        cluster.execute(resetOp);
        resetOp.get();

    }
    
    public static boolean assureIndexExists(String indexName) throws InterruptedException
    {
        for (int x = 0; x < 5; x++)
        {
            Thread.sleep(2000);
            YzFetchIndexOperation fetch = new YzFetchIndexOperation.Builder().withIndexName(indexName).build();
            cluster.execute(fetch);
            fetch.await();
            if (fetch.isSuccess())
            {
                return true;
            }
        }
        
        return false;
    }

    public static Namespace defaultNamespace()
    {
        return new Namespace(testBucketType ? bucketType : BinaryValue.createFromUtf8(Namespace.DEFAULT_BUCKET_TYPE),
                             bucketName);
    }
}
