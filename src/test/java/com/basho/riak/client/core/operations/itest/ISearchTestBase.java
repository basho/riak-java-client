package com.basho.riak.client.core.operations.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.buckets.StoreBucketProperties;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.search.DeleteIndex;
import com.basho.riak.client.api.commands.search.StoreIndex;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.search.YokozunaIndex;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Assert;

import java.util.concurrent.ExecutionException;

import static junit.framework.Assert.assertTrue;

/**
 * Base class for testing Search features.
 *
 * @author Alex Moore  <amoore at basho dot com>
 * @since 2.0.5
 */
public class ISearchTestBase extends ITestBase
{
    public static void setupSearchEnvironment(String bucketName, String indexName) throws ExecutionException, InterruptedException
    {
        final RiakClient client = new RiakClient(cluster);

        if(legacyRiakSearch)
        {
            Namespace legacyNamespace = getLegacyNamespace(bucketName);
            setupLegacySearchBucketProps(client, legacyNamespace);
            setupData(client, legacyNamespace);
        }

        if(testYokozuna && testBucketType)
        {
            Namespace yokoNamespace = getYokoNamespace(bucketName);

            setupYokoIndexAndBucketProps(client, yokoNamespace, indexName);
            setupData(client, yokoNamespace);
        }

        // TODO: figure out a better way than Thread.sleep
        // Give search some time to process everything
        Thread.sleep(3000);
    }

    private static Namespace getLegacyNamespace(String bucketName)
    {
        return new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName);
    }

    private static Namespace getYokoNamespace(String bucketName)
    {
        return new Namespace(yokozunaBucketType.toString(), bucketName);
    }

    public static void cleanupSearchEnvironment(String bucketName, String indexName) throws ExecutionException, InterruptedException
    {
        final RiakClient client = new RiakClient(cluster);

        if(legacyRiakSearch)
        {
            Namespace legacyNamespace = getLegacyNamespace(bucketName);
            cleanupBucket(legacyNamespace);
        }

        if(testYokozuna && testBucketType)
        {
            Namespace yokoNamespace = getYokoNamespace(bucketName);
            cleanupBucket(yokoNamespace);
            breakDownIndex(client, indexName);
        }
    }

    private static void setupLegacySearchBucketProps(RiakClient client, Namespace namespace)
            throws ExecutionException, InterruptedException
    {
        StoreBucketProperties sbp = new StoreBucketProperties.Builder(namespace)
                                        .withLegacyRiakSearchEnabled(true).build();

        final RiakFuture<Void, Namespace> future = client.executeAsync(sbp);
        future.await();
        assertFutureSuccess(future);
    }

    private static void setupYokoIndexAndBucketProps(RiakClient client, Namespace namespace, String indexName)
            throws ExecutionException, InterruptedException
    {
        YokozunaIndex index = new YokozunaIndex(indexName);
        StoreIndex ssi = new StoreIndex.Builder(index).build();

        final RiakFuture<Void, YokozunaIndex> indexCreateFuture = client.executeAsync(ssi);
        indexCreateFuture.await();
        assertFutureSuccess(indexCreateFuture);

        assertTrue("Index " + indexName +  "was not created, check logs.", assureIndexExists(indexName));

        StoreBucketProperties sbp = new StoreBucketProperties.Builder(namespace)
                .withSearchIndex(index.getName())
                .build();
        final RiakFuture<Void, Namespace> propsOp = client.executeAsync(sbp);

        propsOp.await();

        if (!propsOp.isSuccess())
        {
            Assert.assertTrue(propsOp.cause().toString(), propsOp.isSuccess());
        }
    }


    private static void setupData(RiakClient client, Namespace namespace) throws ExecutionException, InterruptedException
    {
        RiakObject ro = new RiakObject()
                .setContentType("application/json")
                .setValue(BinaryValue.create("{\"doc_type_i\":1, \"name_s\":\"Lion-o\", \"age_i\":30, \"leader_b\":true}"));
        Location location = new Location(namespace, "liono");
        StoreValue sv = new StoreValue.Builder(ro).withLocation(location).build();
        client.execute(sv);

        ro = new RiakObject()
                .setContentType("application/json")
                .setValue(BinaryValue.create("{\"doc_type_i\":1, \"name_s\":\"Cheetara\", \"age_i\":28, \"leader_b\":false}"));
        location = new Location(namespace, "cheetara");
        sv = new StoreValue.Builder(ro).withLocation(location).build();
        client.execute(sv);

        ro = new RiakObject()
                .setContentType("application/json")
                .setValue(BinaryValue.create("{\"doc_type_i\":1, \"name_s\":\"Snarf\", \"age_i\":43}"));
        location = new Location(namespace, "snarf");
        sv = new StoreValue.Builder(ro).withLocation(location).build();
        client.execute(sv);

        ro = new RiakObject()
                .setContentType("application/json")
                .setValue(BinaryValue.create("{\"doc_type_i\":1, \"name_s\":\"Panthro\", \"age_i\":36}"));
        location = new Location(namespace, "panthro");
        sv = new StoreValue.Builder(ro).withLocation(location).build();
        client.execute(sv);

        ro = new RiakObject()
                .setContentType("application/json")
                .setValue(BinaryValue.create("{\"doc_type_i\":2, \"content_s\":\"Alice was beginning to get very tired of sitting by her sister on the " +
                                                "bank, and of having nothing to do: once or twice she had peeped into the " +
                                                "book her sister was reading, but it had no pictures or conversations in " +
                                                "it, 'and what is the use of a book,' thought Alice 'without pictures or " +
                                                "conversation?'\"}"));

        location = new Location(namespace, "p1");
        sv = new StoreValue.Builder(ro).withLocation(location).build();
        client.execute(sv);

        ro = new RiakObject()
                .setContentType("application/json")
                .setValue(BinaryValue.create("{\"doc_type_i\":2, \"content_s\":\"So she was considering in her own mind (as well as she could, for the " +
                                                "hot day made her feel very sleepy and stupid), whether the pleasure " +
                                                "of making a daisy-chain would be worth the trouble of getting up and " +
                                                "picking the daisies, when suddenly a White Rabbit with pink eyes ran " +
                                                "close by her.\", \"multi_ss\":[\"this\",\"that\"]}"));

        location = new Location(namespace, "p2");
        sv = new StoreValue.Builder(ro).withLocation(location).build();
        client.execute(sv);

        ro = new RiakObject()
                .setContentType("application/json")
                .setValue(BinaryValue.create("{\"doc_type_i\":2, \"content_s\":\"The rabbit-hole went straight on like a tunnel for some way, and then " +
                                                "dipped suddenly down, so suddenly that Alice had not a moment to think " +
                                                "about stopping herself before she found herself falling down a very deep " +
                                                "well.\"}"));

        location = new Location(namespace, "p3");
        sv = new StoreValue.Builder(ro).withLocation(location).build();
        client.execute(sv);
    }

    private static void cleanupBucket(Namespace namespace) throws ExecutionException, InterruptedException
    {
        resetAndEmptyBucket(namespace);
    }

    private static void breakDownIndex(RiakClient client, String indexName) throws InterruptedException
    {
        DeleteIndex delIdx = new DeleteIndex.Builder(indexName).build();
        final RiakFuture<Void, String> delIdxFuture = client.executeAsync(delIdx);
        delIdxFuture.await();
        assertTrue(delIdxFuture.isSuccess());
    }
}
