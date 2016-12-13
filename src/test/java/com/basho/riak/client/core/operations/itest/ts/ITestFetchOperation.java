package com.basho.riak.client.core.operations.itest.ts;

import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.ts.FetchOperation;
import com.basho.riak.client.core.operations.ts.StoreOperation;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Time Series Fetch Operation Integration Tests
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */

public class ITestFetchOperation extends ITestTsBase
{
    @BeforeClass
    public static void InsertData() throws ExecutionException, InterruptedException
    {
        StoreOperation storeOp = new StoreOperation.Builder(tableName).withRows(rows).build();
        RiakFuture<Void, String> future = cluster.execute(storeOp);

        future.get();
        assertTrue(future.isSuccess());
    }

    @Test
    public void testSingleFetch() throws ExecutionException, InterruptedException
    {
        final Row expectedRow = rows.get(4);
        final List<Cell> keyCells = expectedRow.getCellsCopy().stream().limit(3).collect(Collectors.toList());

        FetchOperation fetchOp = new FetchOperation.Builder(tableName, keyCells).build();

        final RiakFuture<QueryResult, String> future = cluster.execute(fetchOp);

        future.get();
        assertTrue(future.isSuccess());
        QueryResult result = future.get();

        assertEquals(1, result.getRowsCount());
        assertEquals(expectedRow.getCellsCount(), result.getColumnDescriptionsCopy().size());

        Row actualRow = result.getRowsCopy().get(0);
        assertEquals(expectedRow, actualRow);
    }
}
