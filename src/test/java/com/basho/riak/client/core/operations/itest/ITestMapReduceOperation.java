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

import com.basho.riak.client.core.StreamingRiakFuture;
import com.basho.riak.client.core.operations.MapReduceOperation;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class ITestMapReduceOperation extends ITestBase
{
    @BeforeClass
    public static void setup() throws ExecutionException, InterruptedException
    {
        insertData(new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString()));

        if(testBucketType)
        {
            insertData(new Namespace(bucketType, bucketName));
        }
        Thread.sleep(2000);
    }

    @AfterClass
    public static void cleanup() throws ExecutionException, InterruptedException
    {
        resetAndEmptyBucket(new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString()));

        if(testBucketType)
        {
            resetAndEmptyBucket(new Namespace(bucketType, bucketName));
        }
    }

    @Test
    public void testBasicMRDefaultType() throws InterruptedException, ExecutionException, IOException
    {
        // This will currently fail as the 4 arity input in broken in Riak. Specifying
        // "default" doesn't work. I've worked around this in the User API.
        Map<String, Integer> resultMap = testBasicMR(new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString()));
        assertNotNull(resultMap.containsKey("the"));
        assertNull(resultMap.get("the"));
    }

    @Test
    public void testBasicMRTestType() throws InterruptedException, ExecutionException, IOException
    {
        assumeTrue(testBucketType);
        Map<String, Integer> resultMap = testBasicMR(new Namespace(bucketType, bucketName));
        assertNotNull(resultMap.containsKey("the"));
        assertEquals(Integer.valueOf(8), resultMap.get("the"));
    }

    @Test
    public void testBasicStreamingMRTestType() throws IOException, InterruptedException
    {
        assumeTrue(testBucketType);
        Map<Integer, Map<String, Integer>> phaseResultMap = testBasicStreamingMR(new Namespace(bucketType, bucketName));
        Map<String, Integer> resultMap = phaseResultMap.get(1);
        assertNotNull(resultMap.containsKey("the"));
        assertEquals(Integer.valueOf(8), resultMap.get("the"));

    }

    private Map<String, Integer> testBasicMR(Namespace namespace) throws InterruptedException, ExecutionException, IOException
    {
        final ObjectMapper objectMapper = new ObjectMapper();
        String query = createMapReduceQuery(namespace);

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
        @SuppressWarnings("unchecked")
        Map<String, Integer> resultMap = objectMapper.readValue(json, Map.class);

        return resultMap;
    }

    private Map<Integer, Map<String, Integer>>  testBasicStreamingMR(Namespace namespace) throws InterruptedException, IOException
    {
        final String query = createMapReduceQuery(namespace);
        MapReduceOperation mrOp = new MapReduceOperation.Builder(BinaryValue.unsafeCreate(query.getBytes()))
                                                        .streamResults(true)
                                                        .build();

        final StreamingRiakFuture<MapReduceOperation.Response, BinaryValue> streamFuture = cluster.execute(mrOp);

        final TransferQueue<MapReduceOperation.Response> resultsQueue = streamFuture.getResultsQueue();


        Map<Integer, Map<String, Integer>> resultMap = new LinkedHashMap<>();

        while(!streamFuture.isDone())
        {
            final MapReduceOperation.Response response = resultsQueue.poll(10, TimeUnit.MILLISECONDS);
            if(response == null)
            {
                continue;
            }
            mergeWordCountMaps(resultMap, response);
        }

        for (MapReduceOperation.Response response : resultsQueue)
        {
            mergeWordCountMaps(resultMap, response);
        }

        return resultMap;
    }

    private void mergeWordCountMaps(Map<Integer, Map<String, Integer>> resultMap,
                                    MapReduceOperation.Response result) throws IOException
    {
        final ObjectMapper objectMapper = new ObjectMapper();

        for (Integer phaseNumber : result.getResults().keySet())
        {
            resultMap.putIfAbsent(phaseNumber, new LinkedHashMap<>());
            final Map<String, Integer> currentPhaseMap = resultMap.get(phaseNumber);

            final ArrayNode resultList = result.getResults().get(phaseNumber);
            final String wordCountMapJsonString =  resultList.get(0).toString();
            @SuppressWarnings("unchecked")
            Map<String, Integer> wordCountMap = objectMapper.readValue(wordCountMapJsonString, Map.class);

            for(String wordCountKey: wordCountMap.keySet())
            {
                if(currentPhaseMap.containsKey(wordCountKey))
                {
                    final int newWordCountSum = currentPhaseMap.get(wordCountKey) + wordCountMap.get(wordCountKey);
                    currentPhaseMap.put(wordCountKey, newWordCountSum);
                }
                else
                {
                    currentPhaseMap.put(wordCountKey, wordCountMap.get(wordCountKey));
                }
            }
        }
    }

    private String createMapReduceQuery(Namespace namespace)
    {
        final String bucketType = namespace.getBucketTypeAsString();
        final String bucketName = namespace.getBucketNameAsString();

        return "{\"inputs\":[[\"" + bucketName + "\",\"p1\",\"\",\"" + bucketType + "\"]," +
                "[\"" + bucketName + "\",\"p2\",\"\",\"" + bucketType + "\"]," +
                "[\"" + bucketName + "\",\"p3\",\"\",\"" + bucketType + "\"]]," +
                "\"query\":[{\"map\":{\"language\":\"javascript\",\"source\":\"" +
                "function(v) {var m = v.values[0].data.toLowerCase().match(/\\w*/g); var r = [];" +
                "for (var i in m) {if (m[i] != '') {var o = {};o[m[i]] = 1;r.push(o);}}return r;}" +
                "\"}},{\"reduce\":{\"language\":\"javascript\",\"source\":\"" +
                "function(v) {var r = {};for (var i in v) {for(var w in v[i]) {if (w in r) r[w] += v[i][w];" +
                "else r[w] = v[i][w];}}return [r];}\"}}]}";
    }

    private static void insertData(Namespace namespace) throws InterruptedException, ExecutionException
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
    }
}
