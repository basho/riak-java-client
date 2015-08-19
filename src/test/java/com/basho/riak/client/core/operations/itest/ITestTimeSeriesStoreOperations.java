package com.basho.riak.client.core.operations.itest;

import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.TimeSeriesQueryOperation;
import com.basho.riak.client.core.operations.TimeSeriesStoreOperation;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */

public class ITestTimeSeriesStoreOperations extends ITestBase
{

    @Test
    public void writesDataWithoutError() throws ExecutionException, InterruptedException
    {
        final String tableName = "time_series";
        final BinaryValue tableNameBV = BinaryValue.create(tableName);
        final List<Row> rows =
                Arrays.asList(new Row(Arrays.asList(Cell.newTimestampCell(new Date().getTime() - 10),
                                                    Cell.newBinaryCell("bryce"),
                                                    Cell.newNumericCell(305.37))),

                              new Row(Arrays.asList(Cell.newTimestampCell(new Date().getTime() - 5), Cell.newBinaryCell(
                                      "bryce"), Cell.newNumericCell(300.12))),

                              new Row(Arrays.asList(Cell.newTimestampCell(new Date().getTime()), Cell.newBinaryCell(
                                      "bryce"), Cell.newNumericCell(295.95))));


        TimeSeriesStoreOperation storeOp = new TimeSeriesStoreOperation.Builder(tableNameBV).withRows(rows).build();
        RiakFuture<Void, BinaryValue> future = cluster.execute(storeOp);

        future.get();
        assertTrue(future.isSuccess());
    }

}
