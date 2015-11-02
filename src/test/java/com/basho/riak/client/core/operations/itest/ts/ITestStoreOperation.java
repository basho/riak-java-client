package com.basho.riak.client.core.operations.itest.ts;

import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.ts.StoreOperation;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;

/**
 * Time Series Store Operation Integration Tests
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */

public class ITestStoreOperation extends ITestTsBase
{
    @Test
    public void writesDataWithoutError() throws ExecutionException, InterruptedException
    {
        StoreOperation storeOp = new StoreOperation.Builder(tableNameBV).withRows(rows).build();
        RiakFuture<Void, BinaryValue> future = cluster.execute(storeOp);

        future.get();
        assertTrue(future.isSuccess());
    }

}
