package com.basho.riak.client.core.operations.itest.ts;

import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.ts.ListKeysOperation;
import com.basho.riak.client.core.operations.ts.StoreOperation;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
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

public class ITestListKeysOperation extends ITestTsBase
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
        ListKeysOperation listKeysOp = new ListKeysOperation.Builder(tableName).build();

        final RiakFuture<QueryResult, String> future = cluster.execute(listKeysOp);

        future.get();
        assertTrue(future.isSuccess());
        QueryResult result = future.get();

        assertEquals(7, result.getRowsCount());
        assertEquals(0, result.getColumnDescriptionsCopy().size());

        final List<Row> rows = result.getRowsCopy();
        final List<Row> expectedKeys = getKeyHeads();
        // Won't work yet, timestamps come back as sint_64 in list keys
        assertTrue(rows.containsAll(expectedKeys));
    }

    private List<Row> getKeyHeads()
    {
        List<Row> keyHeads = new ArrayList<Row>(rows.size());

        for (Row row : rows)
        {
            final List<Cell> cells = row.getCellsCopy();
            // Hack: replace timestamp cell with sint64 cell.
            keyHeads.add(new Row(cells.get(0), cells.get(1), new Cell(cells.get(2).getTimestamp())));
        }

        return keyHeads;
    }
}
