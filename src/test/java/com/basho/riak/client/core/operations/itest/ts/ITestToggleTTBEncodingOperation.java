package com.basho.riak.client.core.operations.itest.ts;

import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.ToggleTTBEncodingOperation;
import com.basho.riak.client.core.operations.itest.ITestBase;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by srg on 1/12/16.
 */
public class ITestToggleTTBEncodingOperation extends ITestBase {
    @Test
    public void testSingleDelete() throws ExecutionException, InterruptedException
    {
        final boolean useTTB = false;
        final ToggleTTBEncodingOperation op = new ToggleTTBEncodingOperation(useTTB);
        final RiakFuture<ToggleTTBEncodingOperation.Response,String> future = cluster.execute(op);

        final ToggleTTBEncodingOperation.Response response = future.get();
        assertEquals(useTTB, response.isUseNativeEncoding());
    }
}
