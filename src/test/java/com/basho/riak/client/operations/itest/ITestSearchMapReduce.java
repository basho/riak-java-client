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

package com.basho.riak.client.operations.itest;

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.operations.StoreBucketProperties;
import com.basho.riak.client.operations.StoreSearchIndex;
import com.basho.riak.client.operations.kv.StoreValue;
import com.basho.riak.client.operations.mapreduce.MapReduce;
import com.basho.riak.client.operations.mapreduce.SearchMapReduce;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.query.functions.Function;
import com.basho.riak.client.query.search.YokozunaIndex;
import com.basho.riak.client.util.BinaryValue;
import org.junit.Assume;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static junit.framework.Assert.assertEquals;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestSearchMapReduce extends ITestBase
{
    private final RiakClient client = new RiakClient(cluster);
    private final String mrBucketName = bucketName.toString() + "_search_mr";
    
    @Test
    public void serachMR() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testYokozuna);
        Assume.assumeTrue(testBucketType);
        
        // First we have to create an index and attach it to a bucket
        // and the 'default' bucket type can't be used for search
        
        YokozunaIndex index = new YokozunaIndex("test_mr_index");
        StoreSearchIndex ssi = new StoreSearchIndex.Builder(index).build();
        client.execute(ssi);
        
        Location location = new Location(mrBucketName).setBucketType(bucketType);
        StoreBucketProperties sbp = new StoreBucketProperties.Builder(location)
                                    .withSearchIndex(index.getName())
                                    .build();
        client.execute(sbp);
        
        // Without pausing, the index does not propogate in time for the searchop
        // to complete.
        Thread.sleep(3000);
        
        RiakObject ro = new RiakObject()
                        .setContentType("application/json")
                        .setValue(BinaryValue.create("{\"name_s\":\"Lion-o\", \"age_i\":30, \"leader_b\":true}"));
        location.setKey("liono");
        StoreValue sv = new StoreValue.Builder(ro).withLocation(location).build();
        client.execute(sv);
        
        ro = new RiakObject()
                .setContentType("application/json")
                .setValue(BinaryValue.create("{\"name_s\":\"Cheetara\", \"age_i\":28, \"leader_b\":false}"));
        location.setKey("cheetara");
        sv = new StoreValue.Builder(ro).withLocation(location).build();
        client.execute(sv);
        
        ro = new RiakObject()
                .setContentType("application/json")
                .setValue(BinaryValue.create("{\"name_s\":\"Snarf\", \"age_i\":43}"));
        location.setKey("snarf");
        sv = new StoreValue.Builder(ro).withLocation(location).build();
        client.execute(sv);
        
        ro = new RiakObject()
                .setContentType("application/json")
                .setValue(BinaryValue.create("{\"name_s\":\"Panthro\", \"age_i\":36}"));
        location.setKey("panthro");
        sv = new StoreValue.Builder(ro).withLocation(location).build();
        client.execute(sv);
        
        // Sleep some more or ... yeah, it doesn't work.
        Thread.sleep(3000);
        
        SearchMapReduce smr = new SearchMapReduce.Builder()
                            .withBucket(index.getName())
                            .withQuery("NOT leader_b:true")
                            .withMapPhase(Function.newAnonymousJsFunction("function(v) { return [1]; }"), false)
                            .withReducePhase(Function.newNamedJsFunction("Riak.reduceSum"), true)
                            .build();
        
        MapReduce.Response mrResp = client.execute(smr);
        
        assertEquals(3, mrResp.getResultsFromAllPhases().get(0).asInt());
        
        resetAndEmptyBucket(location);
    }
    
}
