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

import com.basho.riak.client.core.operations.SearchOperation;
import com.basho.riak.client.core.operations.StoreBucketPropsOperation;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.core.operations.YzDeleteIndexOperation;
import com.basho.riak.client.core.operations.YzPutIndexOperation;
import static com.basho.riak.client.core.operations.itest.ITestBase.bucketName;
import static com.basho.riak.client.core.operations.itest.ITestBase.cluster;
import static com.basho.riak.client.core.operations.itest.ITestBase.testYokozuna;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.Namespace;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.query.search.YokozunaIndex;
import com.basho.riak.client.util.BinaryValue;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.*;
import org.junit.Assume;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestSearchOperation extends ITestBase
{
    @Test
    public void testSimpleSearch() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(legacyRiakSearch);
        
        Namespace namespace = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());
        StoreBucketPropsOperation op = 
            new StoreBucketPropsOperation.Builder(namespace)
                .withLegacyRiakSearchEnabled(true)
                .build();
        
        cluster.execute(op);
        op.get();
        
        prepSearch(bucketType, bucketName);
        
        SearchOperation searchOp = new SearchOperation.Builder(bucketName, "Alice*").build();
        
        cluster.execute(searchOp);
        SearchOperation.Response result = searchOp.get();
        
        assertEquals(result.numResults(), 2);
        for (Map<String, String> map : result.getAllResults())
        {
            assertFalse(map.isEmpty());
            assertEquals(map.size(), 2); // id and value fields
        }
    }
    
    @Test
    public void testYokozunaSearch() throws InterruptedException, ExecutionException
    {
        //Assume.assumeTrue(testYokozuna);
        
        // First we have to create an index and attach it to a bucket
        // and the 'default' bucket type can't be used for search
        
        BinaryValue searchBucket = BinaryValue.create("search_bucket");
        YokozunaIndex index = new YokozunaIndex("test_index");
        YzPutIndexOperation putOp = new YzPutIndexOperation.Builder(index).build();
        
        cluster.execute(putOp);
        putOp.await();
        
        assertTrue(putOp.isSuccess());
        
        // Without sleeping the bucket props operation will fail saying the 
        // index does not exist.
        Thread.sleep(10000);
        
        Namespace namespace = new Namespace(yokozunaBucketType, searchBucket);
        StoreBucketPropsOperation propsOp = 
            new StoreBucketPropsOperation.Builder(namespace)
                .withSearchIndex("test_index")
                .build();
        cluster.execute(propsOp);
        propsOp.await();
        
        if (!propsOp.isSuccess())
        {
            assertTrue(propsOp.cause().toString(), propsOp.isSuccess());
        }
        
        prepSearch(yokozunaBucketType, searchBucket);
        
        SearchOperation searchOp = new SearchOperation.Builder(BinaryValue.create("test_index"), "text:Alice*").build();
        
        cluster.execute(searchOp);
        searchOp.await();
        assertTrue(searchOp.isSuccess());
        SearchOperation.Response result = searchOp.get();        
        for (Map<String, String> map : result.getAllResults())
        {
            assertFalse(map.isEmpty());
            assertEquals(5, map.size()); // {_yz_rk=p3, _yz_rb=search_bucket, _yz_rt=default, score=1.00000000000000000000e+00, _yz_id=default_search_bucket_p3_25}
        }
        
        resetAndEmptyBucket(namespace);
        
        YzDeleteIndexOperation delOp = new YzDeleteIndexOperation.Builder("test_index").build();
        cluster.execute(delOp);
        delOp.await();
        assertTrue(delOp.isSuccess());
        
    }
    
    private void prepSearch(BinaryValue searchBucketType, BinaryValue searchBucket) throws InterruptedException, ExecutionException
    {

        RiakObject obj = new RiakObject();
                            
        obj.setValue(BinaryValue.create("Alice was beginning to get very tired of sitting by her sister on the " +
                    "bank, and of having nothing to do: once or twice she had peeped into the " +
                    "book her sister was reading, but it had no pictures or conversations in " +
                    "it, 'and what is the use of a book,' thought Alice 'without pictures or " +
                    "conversation?'"));

        Namespace namespace = new Namespace(searchBucketType, searchBucket);
        Location location = new Location(namespace, BinaryValue.unsafeCreate("p1".getBytes()));
        StoreOperation storeOp = 
            new StoreOperation.Builder(location)
                .withContent(obj)
                .build();
        
        cluster.execute(storeOp);
        storeOp.get();
        
        obj.setValue(BinaryValue.create("So she was considering in her own mind (as well as she could, for the " +
                    "hot day made her feel very sleepy and stupid), whether the pleasure " +
                    "of making a daisy-chain would be worth the trouble of getting up and " +
                    "picking the daisies, when suddenly a White Rabbit with pink eyes ran " +
                    "close by her."));
        
        obj.setContentType("text/plain");
        location = new Location(namespace, BinaryValue.unsafeCreate("p2".getBytes()));
        storeOp = 
            new StoreOperation.Builder(location)
                .withContent(obj)
                .build();
        
        cluster.execute(storeOp);
        storeOp.get();
        
        obj.setValue(BinaryValue.create("The rabbit-hole went straight on like a tunnel for some way, and then " +
                    "dipped suddenly down, so suddenly that Alice had not a moment to think " +
                    "about stopping herself before she found herself falling down a very deep " +
                    "well."));
        
        obj.setContentType("text/plain");
        location = new Location(namespace, BinaryValue.unsafeCreate("p3".getBytes()));
        storeOp = 
            new StoreOperation.Builder(location)
                .withContent(obj)
                .build();
        
        cluster.execute(storeOp);
        storeOp.get();
    }
}
