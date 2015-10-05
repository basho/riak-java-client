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
    final String tableName = "GeoCheckin";

    final long now = 1443706900000l; // "now"
    final long fiveMinsInMS = 5l * 60l * 1000l;
    final long fiveMinsAgo = now - fiveMinsInMS;
    final long tenMinsAgo = fiveMinsAgo - fiveMinsInMS;
    final long fifteenMinsAgo = tenMinsAgo - fiveMinsInMS;

    final List<Row> rows = Arrays.asList(
            new Row(new Cell("hash1"), new Cell("user2"), Cell.newTimestamp(fifteenMinsAgo), new Cell("rain"), new Cell(79.0)),
            new Row(new Cell("hash1"), new Cell("user2"), Cell.newTimestamp(fiveMinsAgo), new Cell("wind"),  new Cell(50.5)),
            new Row(new Cell("hash1"), new Cell("user2"), Cell.newTimestamp(now), new Cell("snow"),  new Cell(20.0)));


    @Test
    public void writesDataWithoutError() throws ExecutionException, InterruptedException
    {
        final BinaryValue tableNameBV = BinaryValue.create(tableName);

        TimeSeriesStoreOperation storeOp = new TimeSeriesStoreOperation.Builder(tableNameBV).withRows(rows).build();
        RiakFuture<Void, BinaryValue> future = cluster.execute(storeOp);

        future.get();
        assertTrue(future.isSuccess());
    }

}
