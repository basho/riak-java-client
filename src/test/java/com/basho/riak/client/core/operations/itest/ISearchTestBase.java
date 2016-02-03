package com.basho.riak.client.core.operations.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.buckets.StoreBucketProperties;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.search.StoreIndex;
import com.basho.riak.client.core.operations.YzDeleteIndexOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.search.YokozunaIndex;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Assume;

import java.util.concurrent.ExecutionException;

import static junit.framework.Assert.assertTrue;

/**
 * Created by alex on 2/3/16.
 */
public class ISearchTestBase extends ITestBase
{
    protected static final RiakClient client = new RiakClient(cluster);

    public static void setupSearchEnvironment(String bucketName, String indexName) throws ExecutionException, InterruptedException
    {
        Assume.assumeTrue(testYokozuna);
        Assume.assumeTrue(testBucketType);

        Namespace ns = getSearchNamespace(bucketName);

        setupIndexAndBucketProps(ns, indexName);
        setupData(ns);

        // TODO: figure out a better way than Thread.sleep
        // Give yokozuna some time to process everything
        Thread.sleep(3000);
    }

    public static void cleanupSearchEnvironment(String bucketName, String indexName) throws ExecutionException, InterruptedException
    {
        cleanupBucket(getSearchNamespace(bucketName));
        breakDownIndex(indexName);
    }

    private static Namespace getSearchNamespace(String bucketName)
    {
        return new Namespace(yokozunaBucketType.toString(), bucketName);
    }

    private static void setupIndexAndBucketProps(Namespace namespace, String indexName)
            throws ExecutionException, InterruptedException
    {
        YokozunaIndex index = new YokozunaIndex(indexName);
        StoreIndex ssi = new StoreIndex.Builder(index).build();
        client.execute(ssi);

        assertTrue("Index not created", assureIndexExists(indexName));

        StoreBucketProperties sbp = new StoreBucketProperties.Builder(namespace)
                .withSearchIndex(index.getName())
                .build();
        client.execute(sbp);
    }


    private static void setupData(Namespace namespace) throws ExecutionException, InterruptedException
    {
        RiakObject ro = new RiakObject()
                .setContentType("application/json")
                .setValue(BinaryValue.create("{\"name_s\":\"Lion-o\", \"age_i\":30, \"leader_b\":true}"));
        Location location = new Location(namespace, "liono");
        StoreValue sv = new StoreValue.Builder(ro).withLocation(location).build();
        client.execute(sv);

        ro = new RiakObject()
                .setContentType("application/json")
                .setValue(BinaryValue.create("{\"name_s\":\"Cheetara\", \"age_i\":28, \"leader_b\":false}"));
        location = new Location(namespace, "cheetara");
        sv = new StoreValue.Builder(ro).withLocation(location).build();
        client.execute(sv);

        ro = new RiakObject()
                .setContentType("application/json")
                .setValue(BinaryValue.create("{\"name_s\":\"Snarf\", \"age_i\":43}"));
        location = new Location(namespace, "snarf");
        sv = new StoreValue.Builder(ro).withLocation(location).build();
        client.execute(sv);

        ro = new RiakObject()
                .setContentType("application/json")
                .setValue(BinaryValue.create("{\"name_s\":\"Panthro\", \"age_i\":36}"));
        location = new Location(namespace, "panthro");
        sv = new StoreValue.Builder(ro).withLocation(location).build();
        client.execute(sv);
    }

    private static void cleanupBucket(Namespace namespace) throws ExecutionException, InterruptedException
    {
        resetAndEmptyBucket(namespace);
    }

    private static void breakDownIndex(String indexName) throws InterruptedException
    {
        YzDeleteIndexOperation delOp = new YzDeleteIndexOperation.Builder(indexName).build();
        cluster.execute(delOp);
        delOp.await();
        assertTrue(delOp.isSuccess());
    }

}
