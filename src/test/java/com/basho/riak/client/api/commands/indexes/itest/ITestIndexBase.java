package com.basho.riak.client.api.commands.indexes.itest;

import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.core.query.Namespace;
import org.junit.AfterClass;

import java.util.concurrent.ExecutionException;

public abstract class ITestIndexBase extends ITestBase
{
    protected static Namespace namespace = new Namespace(bucketName.toString());

    @AfterClass
    public static void teardown() throws ExecutionException, InterruptedException
    {
        resetAndEmptyBucket(namespace);
    }
}
