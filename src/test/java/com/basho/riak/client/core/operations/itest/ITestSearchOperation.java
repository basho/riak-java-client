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
import com.basho.riak.client.query.BucketProperties;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.query.search.SearchResult;
import com.basho.riak.client.util.ByteArrayWrapper;
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
        Assume.assumeTrue(riakSearch);
        BucketProperties props = 
            new BucketProperties()
                .withRiakSearchEnabled(true);
        
        StoreBucketPropsOperation op = new StoreBucketPropsOperation(bucketName, props);
        cluster.execute(op);
        op.get();
        
        RiakObject obj = new RiakObject();
                            
        obj.setValue(ByteArrayWrapper.create("Alice was beginning to get very tired of sitting by her sister on the " +
                    "bank, and of having nothing to do: once or twice she had peeped into the " +
                    "book her sister was reading, but it had no pictures or conversations in " +
                    "it, 'and what is the use of a book,' thought Alice 'without pictures or " +
                    "conversation?'"));
        
        StoreOperation storeOp = 
            new StoreOperation.Builder(bucketName)
                .withKey(ByteArrayWrapper.unsafeCreate("p1".getBytes()))
                .withContent(obj)
                .build();
        
        cluster.execute(storeOp);
        storeOp.get();
        
        obj.setValue(ByteArrayWrapper.create("So she was considering in her own mind (as well as she could, for the " +
                    "hot day made her feel very sleepy and stupid), whether the pleasure " +
                    "of making a daisy-chain would be worth the trouble of getting up and " +
                    "picking the daisies, when suddenly a White Rabbit with pink eyes ran " +
                    "close by her."));
        
        storeOp = 
            new StoreOperation.Builder(bucketName)
                .withKey(ByteArrayWrapper.unsafeCreate("p2".getBytes()))
                .withContent(obj)
                .build();
        
        cluster.execute(storeOp);
        storeOp.get();
        
        obj.setValue(ByteArrayWrapper.create("The rabbit-hole went straight on like a tunnel for some way, and then " +
                    "dipped suddenly down, so suddenly that Alice had not a moment to think " +
                    "about stopping herself before she found herself falling down a very deep " +
                    "well."));
        
        storeOp = 
            new StoreOperation.Builder(bucketName)
                .withKey(ByteArrayWrapper.unsafeCreate("p3".getBytes()))
                .withContent(obj)
                .build();
        
        cluster.execute(storeOp);
        storeOp.get();
        
        SearchOperation searchOp = new SearchOperation(bucketName, "Alice*");
        
        cluster.execute(searchOp);
        SearchResult result = searchOp.get();
        
        assertEquals(result.numResults(), 2);
        for (Map<String, String> map : result.getAllResults())
        {
            assertFalse(map.isEmpty());
            assertEquals(map.size(), 2); // id and value fields
        }
    }
}
