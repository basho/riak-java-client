package com.basho.riak.client.core.operations.itest.ts;

import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.ts.ListKeysOperation;
import com.basho.riak.client.core.operations.ts.StoreOperation;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Time Series List Keys Operation Integration Tests
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */

public class ITestListKeysOperations extends ITestTsBase
{
    @BeforeClass
    public static void InsertData() throws ExecutionException, InterruptedException
    {
        StoreOperation storeOp = new StoreOperation.Builder(tableNameBV).withRows(rows).build();
        RiakFuture<Void, BinaryValue> future = cluster.execute(storeOp);

        future.get();
        assertTrue(future.isSuccess());
    }

    @Test
    public void testSingleFetch() throws ExecutionException, InterruptedException
    {
        ListKeysOperation listKeysOp = new ListKeysOperation.Builder(tableNameBV).build();

        final RiakFuture<QueryResult, BinaryValue> future = cluster.execute(listKeysOp);

        future.get();
        assertTrue(future.isSuccess());
        QueryResult result = future.get();

        assertEquals(7, result.getRowsCount());
        assertEquals(0, result.getColumnDescriptionsCopy().size());

        final List<Row> rows = result.getRowsCopy();
        final List<Row> expectedKeys = getKeyHeads();
        assertTrue(rows.containsAll(expectedKeys));
    }

    private List<Row> getKeyHeads()
    {
        List<Row> keyHeads = new ArrayList<Row>(rows.size());

        for (Row row : rows)
        {
            final List<Cell> cells = row.getCellsCopy();
            keyHeads.add(new Row(cells.get(0), cells.get(1), cells.get(2)));
        }

        return keyHeads;
    }
}
