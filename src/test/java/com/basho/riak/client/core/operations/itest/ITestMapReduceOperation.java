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

import com.basho.riak.client.core.operations.MapReduceOperation;
import com.basho.riak.client.core.operations.StoreOperation;
import static com.basho.riak.client.core.operations.itest.ITestBase.bucketName;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.Namespace;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.BinaryValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class ITestMapReduceOperation extends ITestBase
{    
    @Test
    public void testBasicMRDefaultType() throws InterruptedException, ExecutionException, IOException
    {
        // This will currently fail as the 4 arity input in broken in Riak. Specifying 
        // "default" doesn't work. I've worked around this in the User API. 
        testBasicMR(new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString()));
    }
    
    @Test
    public void testBasicMRTestType() throws InterruptedException, ExecutionException, IOException
    {
        assumeTrue(testBucketType);
        testBasicMR(new Namespace(bucketType, bucketName));
    }
    
    private void testBasicMR(Namespace namespace) throws InterruptedException, ExecutionException, IOException
    {
        RiakObject obj = new RiakObject();
                            
        obj.setValue(BinaryValue.create("Alice was beginning to get very tired of sitting by her sister on the " +
                    "bank, and of having nothing to do: once or twice she had peeped into the " +
                    "book her sister was reading, but it had no pictures or conversations in " +
                    "it, 'and what is the use of a book,' thought Alice 'without pictures or " +
                    "conversation?'"));
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
        location = new Location(namespace, BinaryValue.unsafeCreate("p3".getBytes()));
        storeOp = 
            new StoreOperation.Builder(location)
                .withContent(obj)
                .build();
        
        cluster.execute(storeOp);
        storeOp.get();
            
        String bName = bucketName.toString();
        String bType = bucketType.toString();
        
        String query = "{\"inputs\":[[\"" + bName + "\",\"p1\",\"\",\"" + bType + "\"]," +
            "[\"" + bName + "\",\"p2\",\"\",\"" + bType + "\"]," +
            "[\"" + bName + "\",\"p3\",\"\",\"" + bType + "\"]]," +
            "\"query\":[{\"map\":{\"language\":\"javascript\",\"source\":\"" +
            "function(v) {var m = v.values[0].data.toLowerCase().match(/\\w*/g); var r = [];" +
            "for(var i in m) {if(m[i] != '') {var o = {};o[m[i]] = 1;r.push(o);}}return r;}" +
            "\"}},{\"reduce\":{\"language\":\"javascript\",\"source\":\"" +
            "function(v) {var r = {};for(var i in v) {for(var w in v[i]) {if(w in r) r[w] += v[i][w];" +
            "else r[w] = v[i][w];}}return [r];}\"}}]}";
        
        MapReduceOperation mrOp = 
            new MapReduceOperation.Builder(BinaryValue.unsafeCreate(query.getBytes()))
                .build();
        
        cluster.execute(mrOp);
        mrOp.await();
        assertTrue(mrOp.isSuccess());
        ArrayNode resultList = mrOp.get().getResults().get(1);
        
        // The query should return one result which is a JSON array containing a 
        // single JSON object that is a asSet of word counts.
        assertEquals(resultList.size(), 1);
        
        String json = resultList.get(0).toString();
        ObjectMapper oMapper = new ObjectMapper(); 
        @SuppressWarnings("unchecked")
        Map<String, Integer> resultMap = oMapper.readValue(json, Map.class);
        
        assertNotNull(resultMap.containsKey("the"));
        assertEquals(Integer.valueOf(8),resultMap.get("the"));
        
    }
}
