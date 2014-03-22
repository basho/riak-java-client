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

package com.basho.riak.client.operations.itest;

import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.operations.RiakClient;
import com.basho.riak.client.operations.mapreduce.BucketKeyMapReduce;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.query.functions.Function;
import com.basho.riak.client.util.BinaryValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestBucketMapReduce extends ITestBase
{
   
    @Before
    public void insertValues() throws InterruptedException, ExecutionException
    {
        RiakObject obj = new RiakObject();
                            
        obj.setValue(BinaryValue.create("Alice was beginning to get very tired of sitting by her sister on the " +
                    "bank, and of having nothing to do: once or twice she had peeped into the " +
                    "book her sister was reading, but it had no pictures or conversations in " +
                    "it, 'and what is the use of a book,' thought Alice 'without pictures or " +
                    "conversation?'"));
        Location location = new Location(bucketName).setKey(BinaryValue.unsafeCreate("p1".getBytes()));
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
        
        location = new Location(bucketName).setKey(BinaryValue.unsafeCreate("p2".getBytes()));
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
        location = new Location(bucketName).setKey(BinaryValue.unsafeCreate("p3".getBytes()));
        storeOp = 
            new StoreOperation.Builder(location)
                .withContent(obj)
                .build();
        
        cluster.execute(storeOp);
        storeOp.get();
    }
    
    @Test
    public void simpleTest() throws ExecutionException, InterruptedException, IOException
    {
        RiakClient client = new RiakClient(cluster);
        
        String jsMapSource = "function(v) {var m = v.values[0].data.toLowerCase().match(/\\w*/g); var r = [];" +
            "for(var i in m) {if(m[i] != '') {var o = {};o[m[i]] = 1;r.push(o);}}return r;}";
        
        String jsReduceSource = "function(v) {var r = {};for(var i in v) {for(var w in v[i]) {if(w in r) r[w] += v[i][w];" +
            "else r[w] = v[i][w];}}return [r];}";
        
        Function mapFunction = new Function.Builder().withSource(jsMapSource).build();
        Function reduceFunction = new Function.Builder().withSource(jsReduceSource).build();
        
        
        BucketKeyMapReduce bkmr = new BucketKeyMapReduce.Builder()
                            .withLocation(new Location(bucketName).setKey("p1"))
                            .withLocation(new Location(bucketName).setKey("p2"))
                            .withLocation(new Location(bucketName).setKey("p3"))
                            .withMapPhase(mapFunction)
                            .withReducePhase(reduceFunction)
                            .build();
        
        BucketKeyMapReduce.Response resp = client.execute(bkmr);
           
        List<BinaryValue> resultList = resp.getOutput();
        String json = resultList.get(0).toString();
        ObjectMapper oMapper = new ObjectMapper(); 
        @SuppressWarnings("unchecked")
        List<Map<String, Integer>> jsonList = oMapper.readValue(json, List.class);
        Map<String, Integer> resultMap = jsonList.get(0);
        
        assertTrue(resultMap.containsKey("the"));
        assertEquals(resultMap.get("the"), Integer.valueOf(8));
    }
    
}
