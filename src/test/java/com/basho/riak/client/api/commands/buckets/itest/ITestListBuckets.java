package com.basho.riak.client.api.commands.buckets.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.buckets.ListBuckets;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * @author empovit
 * @since 2.0.3
 */
public class ITestListBuckets extends ITestBase
{

    private final RiakClient client = new RiakClient(cluster);

    @Test
    public void testListBucketsDefaultType() throws InterruptedException, ExecutionException
    {
        testListBuckets(Namespace.DEFAULT_BUCKET_TYPE);
    }

    @Test
    public void testListBucketsTestType() throws InterruptedException, ExecutionException
    {
        assumeTrue(testBucketType);
        testListBuckets(bucketType.toString());
    }

    private void testListBuckets(String bucketType) throws InterruptedException, ExecutionException
    {
        // Empty buckets do not show up
        final BinaryValue key = BinaryValue.unsafeCreate("temp_key".getBytes());

        RiakObject value = new RiakObject().setValue(BinaryValue.create("{\"value\":\"value\"}"));

        // Since bucket type in response is populated from the command's context,
        // need a way to make sure the type is indeed as expected - use bucket type for bucket name
        Location location = new Location(new Namespace(bucketType, bucketType), key);
        StoreValue storeCommand = new StoreValue.Builder(value).withLocation(location).build();

        client.execute(storeCommand);

        final BinaryValue typeBinary = BinaryValue.createFromUtf8(bucketType);

        ListBuckets listBucketsCommand = new ListBuckets.Builder(typeBinary).build();

        final ListBuckets.Response listResponse = client.execute(listBucketsCommand);

        Iterator<Namespace> iterator = listResponse.iterator();
        assertTrue(iterator.hasNext());
        boolean found = false;

        while (!found && iterator.hasNext())
        {
            found = iterator.next().getBucketName().equals(typeBinary);
        }

        assertTrue(found);
    }
}
