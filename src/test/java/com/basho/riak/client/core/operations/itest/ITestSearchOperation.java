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

import com.basho.riak.client.core.operations.*;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.search.YokozunaIndex;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @author Alex Moore  <amoore at basho dot com>
 */
public class ITestSearchOperation extends ISearchTestBase
{
    private static final String bucketName = "TestSearchOperation";
    private static final String indexName = "test_index_ITestSearchOperation";

    @BeforeClass
    public static void Setup() throws ExecutionException, InterruptedException
    {
        setupSearchEnvironment(bucketName, indexName);
    }

    @AfterClass
    public static void TearDown() throws ExecutionException, InterruptedException
    {
        cleanupSearchEnvironment(bucketName, indexName);
    }

    @Test
    public void testSimpleSearch() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(legacyRiakSearch);

        SearchOperation searchOp = new SearchOperation.Builder(BinaryValue.create(bucketName), "content_s:Alice*").build();

        cluster.execute(searchOp);
        SearchOperation.Response result = searchOp.get();

        assertEquals(2, result.numResults());
        for (Map<String, List<String>> map : result.getAllResults())
        {
            assertFalse(map.isEmpty());
            assertEquals(3, map.size()); // id, doc_type_i, and content_s fields
        }
    }

    @Test
    public void testYokozunaSearch() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testYokozuna);

        SearchOperation searchOp = new SearchOperation.Builder(BinaryValue.create(indexName), "multi_ss:t*").build();

        cluster.execute(searchOp);
        searchOp.await();
        assertTrue(searchOp.isSuccess());
        SearchOperation.Response result = searchOp.get();
        for (Map<String, List<String>> map : result.getAllResults())
        {
            assertFalse(map.isEmpty());
            assertEquals(8, map.size()); // [_yz_rk, _yz_rb, multi_ss, content_s, _yz_rt, score, _yz_id, doc_type_i]
            assertTrue(map.containsKey("multi_ss"));
            assertEquals(2, map.get("multi_ss").size());
        }
    }
}
