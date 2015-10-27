package com.basho.riak.client.core.operations.itest.ts;

import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.core.operations.ts.DeleteOperation;
import com.basho.riak.client.core.operations.ts.StoreOperation;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;

/**
 * Time Series Delete Operation Integration Tests
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class ITestDeleteOperation extends ITestBase
{
    final String tableName = "GeoCheckin";
    final BinaryValue tableNameBV = BinaryValue.create(tableName);


    final long now = 1443706900000l; // "now"
    final long fiveMinsInMS = 5l * 60l * 1000l;
    final long fiveMinsAgo = now - fiveMinsInMS;
    final long tenMinsAgo = fiveMinsAgo - fiveMinsInMS;
    final long fifteenMinsAgo = tenMinsAgo - fiveMinsInMS;

    final List<Row> rows = Arrays.asList(
            new Row(new Cell("hash2"), new Cell("user4"), Cell.newTimestamp(fifteenMinsAgo), new Cell("rain"), new Cell(79.0)),
            new Row(new Cell("hash2"), new Cell("user4"), Cell.newTimestamp(fiveMinsAgo), new Cell("wind"),  new Cell(50.5)),
            new Row(new Cell("hash2"), new Cell("user4"), Cell.newTimestamp(now), new Cell("snow"),  new Cell(20.0)));

    @BeforeClass
    public static void BeforeClass()
    {
        //Assume.assumeTrue(testTimeSeries);
    }

    @Test
    public void testSingleDelete() throws ExecutionException, InterruptedException
    {
        writeTestData();

        final List<Cell> keyCells = Arrays.asList(new Cell("hash2"), new Cell("user4"), Cell.newTimestamp(fiveMinsAgo));
        DeleteOperation deleteOp = new DeleteOperation.Builder(tableNameBV, keyCells).build();

        final RiakFuture<Void, BinaryValue> future = cluster.execute(deleteOp);

        future.get();
        assertTrue(future.isSuccess());
    }

    private void writeTestData() throws ExecutionException, InterruptedException
    {
        StoreOperation storeOp = new StoreOperation.Builder(tableNameBV).withRows(rows).build();
        RiakFuture<Void, BinaryValue> future = cluster.execute(storeOp);

        future.get();
        assertTrue(future.isSuccess());
    }
}
