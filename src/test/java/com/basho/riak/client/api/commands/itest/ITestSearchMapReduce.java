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
import com.basho.riak.client.api.commands.mapreduce.MapReduce;
import com.basho.riak.client.api.commands.mapreduce.SearchMapReduce;
import com.basho.riak.client.core.operations.itest.ISearchTestBase;
import com.basho.riak.client.core.query.functions.Function;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;


/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestSearchMapReduce extends ISearchTestBase
{
    private final RiakClient client = new RiakClient(cluster);
    private static final String mrBucketName = bucketName.toString() + "_search_mr";
    private static final String indexName = "test_index_ITestSearchMapReduce";

    @BeforeClass
    public static void Setup() throws ExecutionException, InterruptedException
    {
        setupSearchEnvironment(mrBucketName, indexName);
    }

    @AfterClass
    public static void TearDown() throws ExecutionException, InterruptedException
    {
        cleanupSearchEnvironment(mrBucketName, indexName);
    }

    @Test
    public void searchMR() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testYokozuna);
        Assume.assumeTrue(testBucketType);
        Assume.assumeFalse(security);

        SearchMapReduce smr = new SearchMapReduce.Builder()
                .withIndex(indexName)
                .withQuery("doc_type_i:1 AND NOT leader_b:true")
                .withMapPhase(Function.newAnonymousJsFunction("function(v) { return [1]; }"), false)
                .withReducePhase(Function.newNamedJsFunction("Riak.reduceSum"), true)
                .build();

        MapReduce.Response mrResp = client.execute(smr);

        assertEquals(3, mrResp.getResultsFromAllPhases().get(0).asInt());
    }
}
