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
import com.basho.riak.client.core.operations.YzDeleteIndexOperation;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.api.commands.buckets.StoreBucketProperties;
import com.basho.riak.client.api.commands.search.StoreIndex;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.mapreduce.MapReduce;
import com.basho.riak.client.api.commands.mapreduce.SearchMapReduce;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.functions.Function;
import com.basho.riak.client.core.query.search.YokozunaIndex;
import com.basho.riak.client.core.util.BinaryValue;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Assume;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestSearchMapReduce extends ITestBase
{
    private final RiakClient client = new RiakClient(cluster);
    private final String mrBucketName = bucketName.toString() + "_search_mr";
    
    @Test
    public void searchMR() throws InterruptedException, ExecutionException {
        Assume.assumeTrue(testYokozuna);
        Assume.assumeTrue(testBucketType);

        // Search inputs to MR aren't allowed when security is enabled
        Assume.assumeFalse(security);

        // First we have to create an index and attach it to a bucket
        // and the 'default' bucket type can't be used for search

        YokozunaIndex index = new YokozunaIndex("test_mr_index");
        StoreIndex ssi = new StoreIndex.Builder(index).build();
        client.execute(ssi);

        assertTrue("Index not created", assureIndexExists("test_mr_index"));

        Namespace ns = new Namespace(bucketType.toString(), mrBucketName);
        StoreBucketProperties sbp = new StoreBucketProperties.Builder(ns)
                .withSearchIndex(index.getName())
                .build();
        client.execute(sbp);

        RiakObject ro = new RiakObject()
                .setContentType("application/json")
                .setValue(BinaryValue.create("{\"name_s\":\"Lion-o\", \"age_i\":30, \"leader_b\":true}"));
        Location location = new Location(ns, "liono");
        StoreValue sv = new StoreValue.Builder(ro).withLocation(location).build();
        client.execute(sv);

        ro = new RiakObject()
                .setContentType("application/json")
                .setValue(BinaryValue.create("{\"name_s\":\"Cheetara\", \"age_i\":28, \"leader_b\":false}"));
        location = new Location(ns, "cheetara");
        sv = new StoreValue.Builder(ro).withLocation(location).build();
        client.execute(sv);

        ro = new RiakObject()
                .setContentType("application/json")
                .setValue(BinaryValue.create("{\"name_s\":\"Snarf\", \"age_i\":43}"));
        location = new Location(ns, "snarf");
        sv = new StoreValue.Builder(ro).withLocation(location).build();
        client.execute(sv);

        ro = new RiakObject()
                .setContentType("application/json")
                .setValue(BinaryValue.create("{\"name_s\":\"Panthro\", \"age_i\":36}"));
        location = new Location(ns, "panthro");
        sv = new StoreValue.Builder(ro).withLocation(location).build();
        client.execute(sv);

        // Sleep some more or ... yeah, it doesn't work. 
        Thread.sleep(3000);

        SearchMapReduce smr = new SearchMapReduce.Builder()
                .withIndex(index.getName())
                .withQuery("NOT leader_b:true")
                .withMapPhase(Function.newAnonymousJsFunction("function(v) { return [1]; }"), false)
                .withReducePhase(Function.newNamedJsFunction("Riak.reduceSum"), true)
                .build();

        MapReduce.Response mrResp = client.execute(smr);

        assertEquals(3, mrResp.getResultsFromAllPhases().get(0).asInt());

        resetAndEmptyBucket(ns);
        YzDeleteIndexOperation delOp = new YzDeleteIndexOperation.Builder("test_mr_index").build();
        cluster.execute(delOp);
        delOp.await();
        assertTrue(delOp.isSuccess());
    }
    
}
