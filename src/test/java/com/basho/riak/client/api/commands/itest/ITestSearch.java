package com.basho.riak.client.api.commands.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.search.Search;
import com.basho.riak.client.core.operations.SearchOperation;
import com.basho.riak.client.core.operations.itest.ISearchTestBase;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static junit.framework.Assert.assertEquals;

/**
 * Created by alex on 2/3/16.
 */
public class ITestSearch extends ISearchTestBase
{
    private final RiakClient client = new RiakClient(cluster);
    private static final String bucketName = "SearchTest";
    private static final String indexName = "search_index";

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
    public void basicSearch() throws InterruptedException, ExecutionException {
        Assume.assumeTrue(testYokozuna);
        Assume.assumeTrue(testBucketType);

        Search searchCmd = new Search.Builder(indexName, "NOT leader_b:true").build();

        final SearchOperation.Response response = client.execute(searchCmd);
        assertEquals(3, response.numResults());
    }
}
