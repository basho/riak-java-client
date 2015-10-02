package com.basho.riak.client.api.commands.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Query;
import com.basho.riak.client.api.commands.timeseries.Store;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.core.query.timeseries.Cell;
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
    long timestamp3 = 1443796900000l; // "now"
    long timestamp2 = 1443796600000l; // ts3 - 5 mins
    long timestamp1 = 1443796000000l; // ts3 - 15 mins

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

        final String tableName = "GeoCheckin";

        final List<Row> rows = Arrays.asList(
                new Row(new Cell("hash1"), new Cell("user1"), Cell.newTimestamp(timestamp1), new Cell("cloudy"), new Cell(79.0)),
                new Row(new Cell("hash1"), new Cell("user1"), Cell.newTimestamp(timestamp2), new Cell("sunny"),  new Cell(80.5)),
                new Row(new Cell("hash1"), new Cell("user1"), Cell.newTimestamp(timestamp3), new Cell("sunny"),  new Cell(81.0)));

        Store store = new Store.Builder(tableName).withRows(rows).build();

        System.out.println("t1 = " + timestamp1);
        System.out.println("t2 = " + timestamp2);
        System.out.println("t3 = " + timestamp3);

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

        final long now = timestamp3;
        final long tenMinsAgo = timestamp3 - 600000l;


        // Timestamp fields lower bounds are inclusive, upper bounds are exclusive
        // Should only return the 2nd row (one from "5 mins ago")
        // If we added 1 to the "now" time, we would get the third row back too.

        final String queryText = "select * from GeoCheckin " +
                "where user = 'user1' and " +
                "(time > " + tenMinsAgo +" and " +
                "(time < "+ now + ")) ";


        System.out.println(queryText);

        Query query = new Query.Builder(queryText).build();
        QueryResult queryResult = client.execute(query);

        assertEquals(5, queryResult.getColumnDescriptions().size());
        assertEquals(1, queryResult.getRows().size());
        List<Cell> cells = queryResult.getRows().get(0).getCells();

        assertEquals("hash1", cells.get(0).getUtf8String());
        assertEquals("user1", cells.get(1).getUtf8String());
        assertEquals(timestamp2, cells.get(2).getLong());  // idiosyncrasy - need to document or fix
        assertEquals("sunny", cells.get(3).getUtf8String());
        assertEquals(Float.toString(80.5f), Float.toString(cells.get(4).getFloat()));
    }

    @Test
    public void QueryingDataWithExtraPredicate() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        final long now = timestamp3;
        final long tenMinsAgo = timestamp3 - 600000l;


        // Timestamp fields lower bounds are inclusive, upper bounds are exclusive
        // Should only return the 2nd row (one from "5 mins ago")
        // If we added 1 to the "now" time, we would get the third row back too.

        final String queryText = "select * from GeoCheckin " +
                "where user = 'user1' and " +
                "geohash = 'hash1' and " +
                "(time > " + tenMinsAgo +" and " +
                "(time < "+ now + ")) ";


        System.out.println(queryText);

        Query query = new Query.Builder(queryText).build();
        QueryResult queryResult = client.execute(query);

        assertEquals(5, queryResult.getColumnDescriptions().size());
        assertEquals(1, queryResult.getRows().size());
        List<Cell> cells = queryResult.getRows().get(0).getCells();

        assertEquals("hash1", cells.get(0).getUtf8String());
        assertEquals("user1", cells.get(1).getUtf8String());
        assertEquals(timestamp2, cells.get(2).getLong());  // idiosyncrasy - need to document or fix
        assertEquals("sunny", cells.get(3).getUtf8String());
        assertEquals(Float.toString(80.5f), Float.toString(cells.get(4).getFloat()));
    }

    @Test
    public void QueryingDataAcrossManyQuantum() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        final long now = timestamp3;
        final long tenMinsAgo = timestamp3 - 600000l;


        // Timestamp fields lower bounds are inclusive, upper bounds are exclusive
        // Should return the 2nd & 3rd rows. Query should cover 2 quantums too.

        final String queryText = "select * from GeoCheckin " +
                "where user = 'user1' and " +
                "time > " + tenMinsAgo +" and " +
                "time < "+ (now+1) + " ";


        System.out.println(queryText);

        Query query = new Query.Builder(queryText).build();
        QueryResult queryResult = client.execute(query);

        assertEquals(5, queryResult.getColumnDescriptions().size());
        assertEquals(2, queryResult.getRows().size());
        List<Cell> cells = queryResult.getRows().get(0).getCells();

        assertEquals("hash1", cells.get(0).getUtf8String());
        assertEquals("user1", cells.get(1).getUtf8String());
        assertEquals(timestamp2, cells.get(2).getLong());  // idiosyncrasy - need to document or fix
        assertEquals("sunny", cells.get(3).getUtf8String());
        assertEquals(Float.toString(80.5f), Float.toString(cells.get(4).getFloat()));

        cells = queryResult.getRows().get(1).getCells();
        assertEquals("hash1", cells.get(0).getUtf8String());
        assertEquals("user1", cells.get(1).getUtf8String());
        assertEquals(timestamp3, cells.get(2).getLong());  // idiosyncrasy - need to document or fix
        assertEquals("sunny", cells.get(3).getUtf8String());
        assertEquals(Float.toString(81.0f), Float.toString(cells.get(4).getFloat()));
    }

}
