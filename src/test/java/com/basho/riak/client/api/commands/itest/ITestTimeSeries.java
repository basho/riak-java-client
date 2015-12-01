package com.basho.riak.client.api.commands.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Delete;
import com.basho.riak.client.api.commands.timeseries.Fetch;
import com.basho.riak.client.api.commands.timeseries.Query;
import com.basho.riak.client.api.commands.timeseries.Store;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.netty.RiakResponseException;
import com.basho.riak.client.core.operations.itest.ts.ITestTsBase;
import com.basho.riak.client.core.query.timeseries.*;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * Time Series Commands Integration Tests
 *
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
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
 *      uv_index    sint64,
 *      observed    boolean not null,
 *      PRIMARY KEY(
 *          (geohash, user, quantum(time, 15, 'm')),
 *           geohash, user, time)
 *      )
 *   )
 */
public class ITestTimeSeries extends ITestTsBase
{
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
        final String queryText = "select * from GeoCheckin Where time > 1 and time < 10 and user='user1' and geohash='hash1'";
        final IQueryResult queryResult = executeQuery(new Query.Builder(queryText));

        assertNotNull(queryResult);
        assertEquals(0, queryResult.getColumnDescriptions().size());
        assertEquals(0, queryResult.getRowsCount());
    }

    @Test
    public void QueryingDataWithMinimumPredicate() throws ExecutionException, InterruptedException
    {
        // Timestamp fields lower bounds are inclusive, upper bounds are exclusive
        // Should only return the 2nd row (one from "5 mins ago")
        // If we added 1 to the "now" time, we would get the third row back too.

        final String queryText = "select * from GeoCheckin " +
                "where user = 'user1' and " +
                "geohash = 'hash1' and " +
                "(time = " + tenMinsAgo +" and " +
                "(time < "+ now + ")) ";

        final IQueryResult queryResult = executeQuery(new Query.Builder(queryText));

        assertEquals(7, queryResult.getColumnDescriptions().size());
        assertEquals(1, queryResult.getRowsCount());

        assertRowMatches(rows.get(1), queryResult.rows().next());
    }

    @Test
    public void QueryingDataWithExtraPredicate() throws ExecutionException, InterruptedException
    {
        // Timestamp fields lower bounds are inclusive, upper bounds are exclusive
        // Should only return the 2nd row (one from "5 mins ago")
        // If we added 1 to the "now" time, we would get the third row back too.

        final String queryText = "select * from GeoCheckin " +
                "where user = 'user1' and " +
                "geohash = 'hash1' and " +
                "(time > " + tenMinsAgo +" and " +
                "(time < "+ now + ")) ";

        final IQueryResult queryResult = executeQuery(new Query.Builder(queryText));

        assertEquals(7, queryResult.getColumnDescriptions().size());
        assertEquals(1, queryResult.getRowsCount());

        assertRowMatches(rows.get(1), queryResult.rows().next());
    }

    @Test
    public void QueryingDataAcrossManyQuantum() throws ExecutionException, InterruptedException
    {
        // Timestamp fields lower bounds are inclusive, upper bounds are exclusive
        // Should return the 2nd & 3rd rows. Query should cover 2 quantums at least.

        final String queryText = "select * from GeoCheckin " +
                "where user = 'user1' and " +
                "geohash = 'hash1' and " +
                "time > " + tenMinsAgo +" and " +
                "time < "+ fifteenMinsInFuture + " ";

        final IQueryResult queryResult = executeQuery(new Query.Builder(queryText));

        assertEquals(7, queryResult.getColumnDescriptions().size());
        assertEquals(2, queryResult.getRowsCount());

        final Iterator<? extends IRow> itor = queryResult.rows();
        assertRowMatches(rows.get(1), itor.next());
        assertRowMatches(rows.get(2), itor.next());
    }

    @Test
    public void TestThatNullsAreSavedAndFetchedCorrectly() throws ExecutionException, InterruptedException
    {
        final String queryText = "select temperature from GeoCheckin " +
                "where user = 'user2' and " +
                "geohash = 'hash1' and " +
                "(time > " + (fifteenMinsAgo - 1) +" and " +
                "(time < "+ (now + 1) + ")) ";

        final IQueryResult queryResult = executeQuery(new Query.Builder(queryText));

        assertEquals(1, queryResult.getColumnDescriptions().size());
        assertEquals(ColumnDescription.ColumnType.DOUBLE, queryResult.getColumnDescriptions().get(0).getType());

        assertEquals(1, queryResult.getRowsCount());
        final ICell resultCell = queryResult.rows().next().iterator().next();

        assertNull(resultCell);
    }

    @Test
    public void TestQueryingInvalidTableNameResultsInError() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        final String queryText = "select time from GeoChicken";

        Query query = new Query.Builder(queryText).build();
        RiakFuture<IQueryResult, BinaryValue> future = client.executeAsync(query);
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

        final List<Cell> keyCells = Arrays.asList(new Cell("hash2"), new Cell("user4"), Cell.newTimestamp(fifteenMinsAgo));
        Fetch fetch = new Fetch.Builder(tableName, keyCells).build();

        QueryResult queryResult = client.execute(fetch);
        assertEquals(1, queryResult.getRows().size());
        Row row = queryResult.getRows().get(0);
        assertEquals("rain", row.getCells().get(3).getVarcharAsUTF8String());
        assertEquals(79.0, row.getCells().get(4).getDouble(), Double.MIN_VALUE);
    }

    @Test
    public void TestFetchingWithNotFoundKeyReturnsNoRows() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        final List<Cell> keyCells = Arrays.asList(new Cell("nohash"), new Cell("nouser"), Cell.newTimestamp(fifteenMinsAgo));
        Fetch fetch = new Fetch.Builder(tableName, keyCells).build();

        QueryResult queryResult = client.execute(fetch);
        assertEquals(0, queryResult.getRows().size());
    }

    @Test
    public void TestDeletingRowRemovesItFromQueries() throws ExecutionException, InterruptedException
    {
        final List<Cell> keyCells = Arrays.asList(new Cell("hash2"), new Cell("user4"), Cell.newTimestamp(fiveMinsAgo));

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
        QueryResult queryResult2 = client.execute(fetch2);
        assertEquals(0, queryResult2.getRows().size());
    }

    @Test
    public void TestDeletingWithNotFoundKeyDoesNotReturnError() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        final List<Cell> keyCells = Arrays.asList(new Cell("nohash"), new Cell("nouser"), Cell.newTimestamp(fifteenMinsAgo));
        Delete delete = new Delete.Builder(tableName, keyCells).build();

        final RiakFuture<Void, BinaryValue> deleteFuture = client.executeAsync(delete);

        deleteFuture.await();
        assertTrue(deleteFuture.isSuccess());
        assertNull(deleteFuture.cause());
    }

    private static <T> List<T> toList(Iterator<T> itor)
    {
        final List<T> r = new LinkedList<T>();

        while(itor.hasNext())
        {
            r.add(itor.next());
        }
        return r;
    }

    private static <R1 extends IRow, R2 extends IRow> void assertRowMatches(R1 expected, R2 actual)
    {
        List<ICell> expectedCells = toList(expected.iterator());
        List<ICell> actualCells = toList(actual.iterator());

        assertEquals(expectedCells.get(0).getVarcharAsUTF8String(), actualCells.get(0).getVarcharAsUTF8String());
        assertEquals(expectedCells.get(1).getVarcharAsUTF8String(), actualCells.get(1).getVarcharAsUTF8String());
        assertEquals(expectedCells.get(2).getTimestamp(),           actualCells.get(2).getTimestamp());
        assertEquals(expectedCells.get(3).getVarcharAsUTF8String(), actualCells.get(3).getVarcharAsUTF8String());

        ICell expectedCell4 = expectedCells.get(4);
        ICell actualCell4 = actualCells.get(4);

        if (expectedCell4 == null)
        {
            assertNull(actualCell4);
        }
        else
        {
            assertEquals(Double.toString(expectedCells.get(4).getDouble()), Double.toString(actualCells.get(4).getDouble()));
        }

        ICell expectedCell5 = expectedCells.get(5);
        ICell actualCell5 = actualCells.get(5);

        if (expectedCell5 == null)
        {
            assertNull(actualCell5);
        }
        else
        {
            assertEquals(Double.toString(expectedCells.get(5).getLong()), Double.toString(actualCells.get(5).getLong()));
        }

        assertEquals(expectedCells.get(6).getBoolean(),  actualCells.get(6).getBoolean());

    }

}
