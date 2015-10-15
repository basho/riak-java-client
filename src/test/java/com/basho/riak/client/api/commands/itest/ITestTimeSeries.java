package com.basho.riak.client.api.commands.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Query;
import com.basho.riak.client.api.commands.timeseries.Store;
import com.basho.riak.client.core.RiakFuture;
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
 *      temperature float,
 *      PRIMARY KEY (
 *          (quantum(time, 15, 'm'), user),
 *          time, user
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
            new Row(new Cell("hash1"), new Cell("user1"), Cell.newTimestamp(fifteenMinsAgo), new Cell("cloudy"), new Cell(79.0)),
            new Row(new Cell("hash1"), new Cell("user1"), Cell.newTimestamp(fiveMinsAgo), new Cell("sunny"),  new Cell(80.5)),
            new Row(new Cell("hash1"), new Cell("user1"), Cell.newTimestamp(now), new Cell("sunny"),  new Cell(81.0)));


    //    @Test(expected = IllegalArgumentException.class)
    //    public void ensureWeCatchInvalidParams()
    //    {
    //        final String queryText = "select * from GeoCheckin";
    //
    //        Query.Builder query = new Query.Builder(queryText);
    //        query.addStringParameter(":foo", "123");
    //    }

    @Test
    public void StoringData() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        Store store = new Store.Builder(tableName).withRows(rows).build();

        RiakFuture<Void, BinaryValue> execFuture = client.executeAsync(store);

        execFuture.await();
        assertNull(execFuture.cause());
        assertEquals(true, execFuture.isSuccess());
    }

    @Test
    public void QueryingDataNoMatches() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);
        final String queryText = "select * from GeoCheckin Where time > 1 and time < 10 and user ='user1'";
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
                "(time > " + tenMinsAgo +" and " +
                "(time < "+ now + ")) ";

        Query query = new Query.Builder(queryText).build();
        QueryResult queryResult = client.execute(query);

        assertEquals(5, queryResult.getColumnDescriptions().size());
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

        assertEquals(5, queryResult.getColumnDescriptions().size());
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
                "time > " + tenMinsAgo +" and " +
                "time < "+ fifteenMinsInFuture + " ";

        Query query = new Query.Builder(queryText).build();
        QueryResult queryResult = client.execute(query);

        assertEquals(5, queryResult.getColumnDescriptions().size());
        assertEquals(2, queryResult.getRows().size());

        assertRowMatches(rows.get(1), queryResult.getRows().get(0));
        assertRowMatches(rows.get(2), queryResult.getRows().get(1));
    }

    @Test
    public void TestStoringNullableColumns() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        Row noTemp = new Row(new Cell("hash1"), new Cell("user1"), Cell.newTimestamp(now + 1000), new Cell("cloudy"), null);

        Store store = new Store.Builder(tableName).withRow(noTemp).build();

        RiakFuture<Void, BinaryValue> execFuture = client.executeAsync(store);

        execFuture.await();
        assertNull(execFuture.cause().toString(),execFuture.cause());
        assertEquals(true, execFuture.isSuccess());
    }

    @Test
    public void TestThatTimestampsComeBackInIntegerBuffer() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        final String queryText = "select time from GeoCheckin " +
                "where user = 'user1' and " +
                "(time > " + tenMinsAgo +" and " +
                "(time < "+ now + ")) ";

        Query query = new Query.Builder(queryText).build();
        QueryResult queryResult = client.execute(query);

        assertEquals(1, queryResult.getColumnDescriptions().size());
        assertEquals(1, queryResult.getRows().size());
        assertEquals(1, queryResult.getRows().get(0).getCells().size());
        Cell resultCell = queryResult.getRows().get(0).getCells().get(0);
        assertFalse(resultCell.hasTimestamp());
        assertTrue(resultCell.hasLong());
        assertEquals(fiveMinsAgo, resultCell.getLong());
        assertEquals(0, resultCell.getTimestamp());
    }

    private void assertRowMatches(Row expected, Row actual)
    {
        List<Cell> expectedCells = expected.getCells();
        List<Cell> actualCells = actual.getCells();

        assertEquals(expectedCells.get(0).getUtf8String(),  actualCells.get(0).getUtf8String());
        assertEquals(expectedCells.get(1).getUtf8String(),  actualCells.get(1).getUtf8String());
        assertEquals(expectedCells.get(2).getTimestamp(),   actualCells.get(2).getLong());  // idiosyncrasy - need to document or fix
        assertEquals(expectedCells.get(3).getUtf8String(),  actualCells.get(3).getUtf8String());
        assertEquals(Float.toString(expectedCells.get(4).getFloat()), Float.toString(actualCells.get(4).getFloat()));
    }

}
