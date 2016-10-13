package com.basho.riak.client.api.commands.buckets.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.ListKeys;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Iterator;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;


public class ITestListKeys extends ITestBase
{
    private static final RiakClient client = new RiakClient(cluster);
    private static final String bucketName = "ITestListBuckets" + new Random().nextLong();
    private static final Namespace typedNamespace = new Namespace(bucketType.toString(), bucketName);

    @BeforeClass
    public static void setup() throws ExecutionException, InterruptedException
    {
        if(testBucketType)
        {
            storeTestObjects(typedNamespace);
        }
    }

    @AfterClass
    public static void cleanup() throws ExecutionException, InterruptedException
    {
        if(testBucketType)
        {
            resetAndEmptyBucket(typedNamespace);
        }
    }

    @Test
    public void testLargeStreamingListKeys() throws ExecutionException, InterruptedException
    {
        assumeTrue(testBucketType);

        ListKeys lk = new ListKeys.Builder(typedNamespace).build();

        final RiakFuture<ListKeys.StreamingResponse, Namespace> streamFuture =
                client.executeAsyncStreaming(lk, 200);

        final ListKeys.StreamingResponse streamingResponse = streamFuture.get();

        int count = 0;
        boolean foundLastKey = false;

        for (Location location : streamingResponse)
        {
            if(location == null)
            {
                continue;
            }

            count++;

            if(!foundLastKey)
            {
                foundLastKey = location.getKeyAsString().equals("9999");
            }
        }

        streamFuture.await();
        assertTrue(foundLastKey);
        assertTrue(streamFuture.isDone());
        assertEquals(10000, count);
    }

    private static void storeTestObjects(Namespace namespace) throws InterruptedException
    {
        final String value = "{\"value\":\"value\"}";
        final RiakObject rObj = new RiakObject().setValue(BinaryValue.create(value));

        for (int i = 0; i < 10000; i++)
        {
            final String key = Integer.toString(i);

            final Location location = new Location(namespace, key);
            final StoreOperation storeOp =
                    new StoreOperation.Builder(location)
                            .withContent(rObj)
                            .build();

            final RiakFuture<StoreOperation.Response, Location> execute = cluster.execute(storeOp);
            execute.await();
            assertTrue(execute.isSuccess());
        }
    }
}
