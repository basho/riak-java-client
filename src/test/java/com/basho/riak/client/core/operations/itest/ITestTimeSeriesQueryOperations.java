package com.basho.riak.client.core.operations.itest;

import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.TimeSeriesQueryOperation;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */

public class ITestTimeSeriesQueryOperations extends ITestBase
{

    @Test
    public void creatingCollections() throws ExecutionException, InterruptedException {
        final String tableName = "time_series";
        final String queryText = String.format(
                "create table %s (" +
                "time timestamp not null," +
                "user_id varchar not null," +
                "temperature_k float," +
                "primary key (time))",
                tableName);

        final BinaryValue queryTextBS = BinaryValue.create(queryText);

        TimeSeriesQueryOperation queryOp = new TimeSeriesQueryOperation.Builder(queryTextBS).build();
        RiakFuture<QueryResult, BinaryValue> future = cluster.execute(queryOp);

        QueryResult queryResult = future.get();
        assertTrue(future.isSuccess());

        // These will change, putting test here to demonstrate that we are converting the types correctly.
        assertEquals(1, queryResult.getColumnDescriptions().size());
        assertEquals("asdf", queryResult.getColumnDescriptions().get(0).getName());

        List<Row> rows = queryResult.getRows();
        assertEquals(1, rows.size());
        List<Cell> cells = rows.get(0).getCells();
        assertEquals(1, cells.size());
        Cell cell = cells.get(0);
        assertTrue(cell.hasBinaryValue());
        assertEquals("jkl;", cell.getBinaryValue().toStringUtf8());
    }

}
