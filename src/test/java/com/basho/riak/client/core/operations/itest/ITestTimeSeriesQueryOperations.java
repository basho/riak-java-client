package com.basho.riak.client.core.operations.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Query;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.TimeSeriesQueryOperation;
import com.basho.riak.client.core.operations.TimeSeriesStoreOperation;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.client.core.util.BinaryValue;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Time Series Query Operation Integration Tests
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */

public class ITestTimeSeriesQueryOperations extends ITestBase
{
    final static String tableName = "GeoCheckin";

    final static long now = 1443806900000l; // "now"
    final static long fiveMinsInMS = 5l * 60l * 1000l;
    final static long fiveMinsAgo = now - fiveMinsInMS;
    final static long tenMinsAgo = fiveMinsAgo - fiveMinsInMS;
    final static long fifteenMinsAgo = tenMinsAgo - fiveMinsInMS;

    final static  List<Row> rows = Arrays.asList(
            new Row(new Cell("hash1"), new Cell("user2"), Cell.newTimestamp(fifteenMinsAgo), new Cell("rain"), new Cell(79.0)),
            new Row(new Cell("hash1"), new Cell("user2"), Cell.newTimestamp(fiveMinsAgo), new Cell("wind"),  new Cell(50.5)),
            new Row(new Cell("hash1"), new Cell("user2"), Cell.newTimestamp(now), new Cell("snow"),  new Cell(20.0)));

    @BeforeClass
    public static void InsertData() throws ExecutionException, InterruptedException
    {
        Assert.assertTrue(testTimeSeries);
        final BinaryValue tableNameBV = BinaryValue.create(tableName);

        TimeSeriesStoreOperation storeOp = new TimeSeriesStoreOperation.Builder(tableNameBV).withRows(rows).build();
        RiakFuture<Void, BinaryValue> future = cluster.execute(storeOp);

        future.get();
        assertTrue(future.isSuccess());
    }

    @Test
    public void queryNoMatches() throws ExecutionException, InterruptedException
    {
        final String queryText = "select * from GeoCheckin " +
                                 "where time > 0 " +
                                 "  and time < 10 " +
                                 "  and user ='user1'";

        final BinaryValue queryTextBS = BinaryValue.create(queryText);

        TimeSeriesQueryOperation queryOp = new TimeSeriesQueryOperation.Builder(queryTextBS).build();
        RiakFuture<QueryResult, BinaryValue> future = cluster.execute(queryOp);

        QueryResult queryResult = future.get();
        assertTrue(future.isSuccess());

        assertNotNull(queryResult);
        assertEquals(0, queryResult.getColumnDescriptions().size());
        assertEquals(0, queryResult.getRows().size());
    }

    @Test
    public void querySomeMatches() throws ExecutionException, InterruptedException
    {
        final String queryText = "select * from GeoCheckin " +
                                 "where time > " + tenMinsAgo +
                                 "  and time < "+ now +
                                 "  and user ='user2'";
        final BinaryValue queryTextBS = BinaryValue.create(queryText);

        TimeSeriesQueryOperation queryOp = new TimeSeriesQueryOperation.Builder(queryTextBS).build();
        RiakFuture<QueryResult, BinaryValue> future = cluster.execute(queryOp);

        QueryResult queryResult = future.get();
        assertTrue(future.isSuccess());

        assertNotNull(queryResult);
        assertEquals(5, queryResult.getColumnDescriptions().size());
        assertEquals(1, queryResult.getRows().size());
    }
}
