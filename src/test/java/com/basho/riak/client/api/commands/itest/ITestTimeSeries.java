package com.basho.riak.client.api.commands.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Delete;
import com.basho.riak.client.api.commands.timeseries.Fetch;
import com.basho.riak.client.api.commands.timeseries.Query;
import com.basho.riak.client.api.commands.timeseries.Store;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.netty.RiakResponseException;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.ColumnDescription;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * Time Series Commands Integration Tests
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 *
 * Schema for the Timeseries table we're using:
 *
 *   CREATE TABLE GeoCheckin
 *   (
 *      geohash     varchar   not null,
 *      user        varchar   not null,
 *      time        timestamp not null,
 *      weather     varchar   not null,
 *      temperature double,
 *      uv_index    sint64
 *      PRIMARY KEY(
 *          (geohash, user, quantum(time, 15, 'm')),
 *           geohash, user, time)
 *      )
 *   )
 */
public class ITestTimeSeries extends ITestBase
{
    final String tableName = "GeoCheckin";

    final long now = 1443796900000l; // "now"
    final long fiveMinsInMS = 5l * 60l * 1000l;
    final long fiveMinsAgo = now - fiveMinsInMS;
    final long tenMinsAgo = fiveMinsAgo - fiveMinsInMS;
    final long fifteenMinsAgo = tenMinsAgo - fiveMinsInMS;
    final long fifteenMinsInFuture = now + (fiveMinsInMS * 3l);

    final List<Row> rows = Arrays.asList(
            // "Normal" Data
            new Row(new Cell("hash1"), new Cell("user1"), Cell.newTimestamp(fifteenMinsAgo), new Cell("cloudy"), new Cell(79.0), new Cell(1)),
            new Row(new Cell("hash1"), new Cell("user1"), Cell.newTimestamp(fiveMinsAgo), new Cell("sunny"),  new Cell(80.5), new Cell(2)),
            new Row(new Cell("hash1"), new Cell("user1"), Cell.newTimestamp(now), new Cell("sunny"),  new Cell(81.0), new Cell(10)),

            // Null Cell row
            new Row(new Cell("hash1"), new Cell("user2"), Cell.newTimestamp(fiveMinsAgo), new Cell("cloudy"), null, null),

            // Data for single reads / deletes
            new Row(new Cell("hash1"), new Cell("user4"), Cell.newTimestamp(fifteenMinsAgo), new Cell("rain"), new Cell(79.0), new Cell(2)),
            new Row(new Cell("hash1"), new Cell("user4"), Cell.newTimestamp(fiveMinsAgo), new Cell("wind"),  new Cell(50.5), new Cell(3)),
            new Row(new Cell("hash1"), new Cell("user4"), Cell.newTimestamp(now), new Cell("snow"),  new Cell(20.0), new Cell(11)));

//    @BeforeClass
//    public static void BeforeClass()
//    {
//        Assume.assumeTrue(testTimeSeries);
//    }

    @Test
    public void StoringData() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        Store store = new Store.Builder(tableName).withRows(rows).build();

        RiakFuture<Void, BinaryValue> execFuture = client.executeAsync(store);

        execFuture.await();
        String errorMessage = execFuture.cause() != null? execFuture.cause().getMessage() : "";
        assertNull(errorMessage, execFuture.cause());
        assertEquals(true, execFuture.isSuccess());
    }

    @Test
    public void QueryingDataNoMatches() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);
        final String queryText = "select * from GeoCheckin Where time > 1 and time < 10 and user='user1' and geohash='hash1'";
        Query query = new Query.Builder(queryText).build();
        QueryResult queryResult = client.execute(query);
        assertNotNull(queryResult);
        assertEquals(0, queryResult.getColumnDescriptions().size());
        assertEquals(0, queryResult.getRows().size());
    }

    @Test
    public void QueryingDataWithMinimumPredicate() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        // Timestamp fields lower bounds are inclusive, upper bounds are exclusive
        // Should only return the 2nd row (one from "5 mins ago")
        // If we added 1 to the "now" time, we would get the third row back too.

        final String queryText = "select * from GeoCheckin " +
                "where user = 'user1' and " +
                "geohash = 'hash1' and " +
                "(time = " + tenMinsAgo +" and " +
                "(time < "+ now + ")) ";

        Query query = new Query.Builder(queryText).build();
        QueryResult queryResult = client.execute(query);

        assertEquals(6, queryResult.getColumnDescriptions().size());
        assertEquals(1, queryResult.getRows().size());

        assertRowMatches(rows.get(1), queryResult.getRows().get(0));
    }

    @Test
    public void QueryingDataWithExtraPredicate() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        // Timestamp fields lower bounds are inclusive, upper bounds are exclusive
        // Should only return the 2nd row (one from "5 mins ago")
        // If we added 1 to the "now" time, we would get the third row back too.

        final String queryText = "select * from GeoCheckin " +
                "where user = 'user1' and " +
                "geohash = 'hash1' and " +
                "(time > " + tenMinsAgo +" and " +
                "(time < "+ now + ")) ";

        Query query = new Query.Builder(queryText).build();
        QueryResult queryResult = client.execute(query);

        assertEquals(6, queryResult.getColumnDescriptions().size());
        assertEquals(1, queryResult.getRows().size());

        assertRowMatches(rows.get(1), queryResult.getRows().get(0));
    }

    @Test
    public void QueryingDataAcrossManyQuantum() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        // Timestamp fields lower bounds are inclusive, upper bounds are exclusive
        // Should return the 2nd & 3rd rows. Query should cover 2 quantums at least.

        final String queryText = "select * from GeoCheckin " +
                "where user = 'user1' and " +
                "geohash = 'hash1' and " +
                "time > " + tenMinsAgo +" and " +
                "time < "+ fifteenMinsInFuture + " ";

        Query query = new Query.Builder(queryText).build();
        QueryResult queryResult = client.execute(query);

        assertEquals(6, queryResult.getColumnDescriptions().size());
        assertEquals(2, queryResult.getRows().size());

        assertRowMatches(rows.get(1), queryResult.getRows().get(0));
        assertRowMatches(rows.get(2), queryResult.getRows().get(1));
    }

    @Test
    public void TestThatNullsAreSavedAndFetchedCorrectly() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        final String queryText = "select temperature from GeoCheckin " +
                "where user = 'user2' and " +
                "geohash = 'hash1' and " +
                "(time > " + (fifteenMinsAgo - 1) +" and " +
                "(time < "+ (now + 1) + ")) ";

        Query query = new Query.Builder(queryText).build();
        QueryResult queryResult = client.execute(query);

        assertEquals(1, queryResult.getColumnDescriptions().size());
        assertEquals(ColumnDescription.ColumnType.DOUBLE, queryResult.getColumnDescriptions().get(0).getType());

        assertEquals(1, queryResult.getRows().size());
        Cell resultCell = queryResult.getRows().get(0).getCells().get(0);

        assertNull(resultCell);
    }

    @Test
    public void TestQueryingInvalidTableNameResultsInError() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        final String queryText = "select time from GeoChicken";

        Query query = new Query.Builder(queryText).build();
        RiakFuture<QueryResult, BinaryValue> future = client.executeAsync(query);
        future.await();
        assertFalse(future.isSuccess());
        assertEquals(future.cause().getClass(), RiakResponseException.class);
    }

    @Test
    public void TestStoringDataOutOfOrderResultsInError() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        Row row = new Row(Cell.newTimestamp(fifteenMinsAgo), new Cell("hash1"), new Cell("user1"), new Cell("cloudy"), new Cell(79.0));
        Store store = new Store.Builder("GeoChicken").withRow(row).build();

        RiakFuture<Void, BinaryValue> future = client.executeAsync(store);
        future.await();
        assertFalse(future.isSuccess());
        assertEquals(future.cause().getClass(), RiakResponseException.class);
    }

    @Test
    public void TestFetchingSingleRowsWorks() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        final List<Cell> keyCells = Arrays.asList(new Cell("hash1"), new Cell("user4"), Cell.newTimestamp(fifteenMinsAgo));
        Fetch fetch = new Fetch.Builder(tableName, keyCells).build();

        QueryResult queryResult = client.execute(fetch);
        assertEquals(1, queryResult.getRows().size());
        Row row = queryResult.getRows().get(0);
        assertEquals("rain", row.getCells().get(3).getUtf8String());
        assertEquals(79.0, row.getCells().get(4).getDouble(), Double.MIN_VALUE);
    }

    @Test
    public void TestDeletingRowRemovesItFromQueries() throws ExecutionException, InterruptedException
    {
        final List<Cell> keyCells = Arrays.asList(new Cell("hash1"), new Cell("user4"), Cell.newTimestamp(fiveMinsAgo));

        RiakClient client = new RiakClient(cluster);

        // Assert we have a row
        Fetch fetch = new Fetch.Builder(tableName, keyCells).build();
        QueryResult queryResult = client.execute(fetch);
        assertEquals(1, queryResult.getRows().size());

        // Delete row
        Delete delete = new Delete.Builder(tableName, keyCells).build();

        final RiakFuture<Void, BinaryValue> deleteFuture = client.executeAsync(delete);

        deleteFuture.await();
        assertTrue(deleteFuture.isSuccess());
        assertNull(deleteFuture.cause());

        // Assert that the row is no longer with us
        Fetch fetch2 = new Fetch.Builder(tableName, keyCells).build();
        //QueryResult queryResult2 = client.execute(fetch2);
        //assertEquals(0, queryResult2.getRows().size());

        // NB: This is the expected behavior as of 2015-11-02.
        // Want to move it to return a normal response with 0 rows instead.
        RiakFuture<QueryResult, BinaryValue> fetchFuture = client.executeAsync(fetch2);
        fetchFuture.await();
        assertFalse(fetchFuture.isSuccess());
        assertEquals(fetchFuture.cause().getClass(), RiakResponseException.class);
        assertEquals(fetchFuture.cause().getMessage(), "notfound");
    }

    private void assertRowMatches(Row expected, Row actual)
    {
        List<Cell> expectedCells = expected.getCells();
        List<Cell> actualCells = actual.getCells();

        assertEquals(expectedCells.get(0).getUtf8String(),  actualCells.get(0).getUtf8String());
        assertEquals(expectedCells.get(1).getUtf8String(),  actualCells.get(1).getUtf8String());
        assertEquals(expectedCells.get(2).getTimestamp(),   actualCells.get(2).getTimestamp());
        assertEquals(expectedCells.get(3).getUtf8String(),  actualCells.get(3).getUtf8String());

        Cell expectedCell4 = expectedCells.get(4);
        Cell actualCell4 = actualCells.get(4);

        if (expectedCell4 == null)
        {
            assertNull(actualCell4);
        }
        else
        {
            assertEquals(Double.toString(expectedCells.get(4).getDouble()), Double.toString(actualCells.get(4).getDouble()));
        }

        Cell expectedCell5 = expectedCells.get(5);
        Cell actualCell5 = actualCells.get(5);

        if (expectedCell5 == null)
        {
            assertNull(actualCell5);
        }
        else
        {
            assertEquals(Double.toString(expectedCells.get(5).getLong()), Double.toString(actualCells.get(5).getLong()));
        }
    }

}
