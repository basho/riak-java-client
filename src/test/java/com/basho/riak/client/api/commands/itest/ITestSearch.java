package com.basho.riak.client.api.commands.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.search.Search;
import com.basho.riak.client.core.operations.SearchOperation;
import com.basho.riak.client.core.operations.itest.ISearchTestBase;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

/**
 * Created by alex on 2/3/16.
 */
public class ITestSearch extends ISearchTestBase
{
    private final RiakClient client = new RiakClient(cluster);
    private static final String bucketName = "SearchTestCommand";
    private static final String indexName = "search_index_ITestSearch";
    private final String thundercatsQuery = "doc_type_i:1 AND NOT leader_b:true";


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
    public void basicSearch() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testYokozuna);
        Assume.assumeTrue(testBucketType);

        Search searchCmd = new Search.Builder(indexName, thundercatsQuery).build();

        final SearchOperation.Response response = client.execute(searchCmd);
        assertEquals(3, response.numResults());
    }

    @Test
    public void testZeroSizedPages() throws ExecutionException, InterruptedException
    {
        Assume.assumeTrue(testYokozuna);
        Assume.assumeTrue(testBucketType);

        Search searchCmd = new Search.Builder(indexName, thundercatsQuery).withRows(0).build();

        final SearchOperation.Response response = client.execute(searchCmd);
        assertEquals(3, response.numResults());
        assertEquals(0, response.getAllResults().size());
    }

    @Test
    public void testSortField() throws ExecutionException, InterruptedException
    {
        Assume.assumeTrue(testYokozuna);
        Assume.assumeTrue(testBucketType);

        Search searchCmd = new Search.Builder(indexName, thundercatsQuery).sort("age_i asc").build();

        final SearchOperation.Response response = client.execute(searchCmd);
        assertEquals(3, response.numResults());

        ArrayList<String> ages = new ArrayList<String>();
        for (Map<String, List<String>> stringListMap : response.getAllResults())
        {
            ages.add(stringListMap.get("age_i").get(0));
        }

        assertArrayEquals(new String[]{"28", "36", "43"}, ages.toArray());
    }
}
