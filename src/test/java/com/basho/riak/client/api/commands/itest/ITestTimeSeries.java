package com.basho.riak.client.api.commands.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.buckets.FetchBucketProperties;
import com.basho.riak.client.api.commands.buckets.StoreBucketProperties;
import com.basho.riak.client.api.commands.timeseries.*;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.netty.RiakResponseException;
import com.basho.riak.client.core.operations.FetchBucketPropsOperation;
import com.basho.riak.client.core.operations.itest.ts.ITestTsBase;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.timeseries.*;
import org.junit.Test;

import java.text.MessageFormat;
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
    private final static String tableName = "GeoHash" + new Random().nextInt(Integer.MAX_VALUE);

    @Test
    public void TestCreateTableAndChangeNVal() throws Exception
    {
        RiakClient client = new RiakClient(cluster);

        String createTableCommandString =
            "CREATE TABLE " + this.tableName +
            " ( " +
            "   geohash varchar not null, " +
            "   user varchar not null, " +
            "   time timestamp not null, " +
            "   weather varchar not null, " +
            "   temperature double, " +
            "   uv_index sint64, " +
            "   observed boolean not null, " +
            "   PRIMARY KEY((geohash, user, quantum(time, 15, 'm')), geohash, user, time))";

        Query create = new Query.Builder(createTableCommandString).build();
        final RiakFuture<QueryResult, String> resultFuture = client.executeAsync(create);

        resultFuture.await();
        if(resultFuture.cause() != null)
        {
            assertTrue(resultFuture.cause().getMessage(), resultFuture.isSuccess());
        }


        final Namespace namespace = new Namespace(tableName, tableName);
        StoreBucketProperties storeBucketPropsCmd = new StoreBucketProperties.Builder(namespace).withNVal(1).build();
        final RiakFuture<Void, Namespace> storeBucketPropsFuture = client.executeAsync(storeBucketPropsCmd);

        storeBucketPropsFuture.await();
        assertTrue(storeBucketPropsFuture.isSuccess());
        assertNull(resultFuture.cause());

        FetchBucketProperties fetchBucketPropsCmd = new FetchBucketProperties.Builder(namespace).build();
        final RiakFuture<FetchBucketPropsOperation.Response, Namespace> getBucketPropsFuture =
                client.executeAsync(fetchBucketPropsCmd);

        getBucketPropsFuture.await();
        assertTrue(getBucketPropsFuture.isSuccess());
        assertNull(getBucketPropsFuture.cause());
        assertTrue(1 == getBucketPropsFuture.get().getBucketProperties().getNVal());
    }

    @Test
    public void StoringData() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        Store store = new Store.Builder(tableName).withRows(rows).build();

        RiakFuture<Void, String> execFuture = client.executeAsync(store);

        execFuture.await();
        String errorMessage = execFuture.cause() != null? execFuture.cause().getMessage() : "";
        assertNull(errorMessage, execFuture.cause());
        assertEquals(true, execFuture.isSuccess());
    }

    @Test
    public void TestListingKeysReturnsThem() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        ListKeys listKeys = new ListKeys.Builder(tableName).build();

        final RiakFuture<QueryResult, String> listKeysFuture = client.executeAsync(listKeys);

        listKeysFuture.await();
        assertTrue(listKeysFuture.isSuccess());
        assertNull(listKeysFuture.cause());

        final QueryResult queryResult = listKeysFuture.get();
        assertTrue(queryResult.getRowsCount() > 0);
    }

    @Test
    public void QueryingDataNoMatches() throws ExecutionException, InterruptedException
    {
        final String queryText = "select * from " + tableName + " Where time > 1 and time < 10 and user='user1' and geohash='hash1'";
        final QueryResult queryResult = executeQuery(new Query.Builder(queryText));

        assertNotNull(queryResult);
        assertEquals(0, queryResult.getColumnDescriptionsCopy().size());
        assertEquals(0, queryResult.getRowsCount());
    }

    @Test
    public void QueryingDataWithMinimumPredicate() throws ExecutionException, InterruptedException
    {
        // Timestamp fields lower bounds are inclusive, upper bounds are exclusive
        // Should only return the 2nd row (one from "5 mins ago")
        // If we added 1 to the "now" time, we would get the third row back too.

        final String queryText = "select * from " + tableName + " " +
                "where user = 'user1' and " +
                "geohash = 'hash1' and " +
                "(time > " + tenMinsAgo +" and " +
                "(time < "+ now + ")) ";

        final QueryResult queryResult = executeQuery(new Query.Builder(queryText));

        assertEquals(7, queryResult.getColumnDescriptionsCopy().size());
        assertEquals(1, queryResult.getRowsCount());

        assertRowMatches(rows.get(1), queryResult.iterator().next());
    }

    @Test
    public void QueryingDataWithExtraPredicate() throws ExecutionException, InterruptedException
    {
        // Timestamp fields lower bounds are inclusive, upper bounds are exclusive
        // Should only return the 2nd row (one from "5 mins ago")
        // If we added 1 to the "now" time, we would get the third row back too.

        final String queryText = "select * from " + tableName + " " +
                "where user = 'user1' and " +
                "geohash = 'hash1' and " +
                "(time > " + tenMinsAgo +" and " +
                "(time < "+ now + ")) ";

        final QueryResult queryResult = executeQuery(new Query.Builder(queryText));

        assertEquals(7, queryResult.getColumnDescriptionsCopy().size());
        assertEquals(1, queryResult.getRowsCount());

        assertRowMatches(rows.get(1), queryResult.iterator().next());
    }

    @Test
    public void QueryingDataAcrossManyQuantum() throws ExecutionException, InterruptedException
    {
        // Timestamp fields lower bounds are inclusive, upper bounds are exclusive
        // Should return the 2nd & 3rd rows. Query should cover 2 quantums at least.

        final String queryText = "select * from " + tableName + " " +
                "where user = 'user1' and " +
                "geohash = 'hash1' and " +
                "time > " + tenMinsAgo +" and " +
                "time < "+ fifteenMinsInFuture + " ";

        final QueryResult queryResult = executeQuery(new Query.Builder(queryText));

        assertEquals(7, queryResult.getColumnDescriptionsCopy().size());
        assertEquals(2, queryResult.getRowsCount());

        final Iterator<? extends Row> itor = queryResult.iterator();
        assertRowMatches(rows.get(1), itor.next());
        assertRowMatches(rows.get(2), itor.next());
    }

    @Test
    public void TestThatNullsAreSavedAndFetchedCorrectly() throws ExecutionException, InterruptedException
    {
        final String queryText = "select temperature from " + tableName + " " +
                "where user = 'user2' and " +
                "geohash = 'hash1' and " +
                "(time > " + (fifteenMinsAgo - 1) +" and " +
                "(time < "+ (now + 1) + ")) ";

        final QueryResult queryResult = executeQuery(new Query.Builder(queryText));

        assertEquals(1, queryResult.getColumnDescriptionsCopy().size());
        assertEquals(ColumnDescription.ColumnType.DOUBLE, queryResult.getColumnDescriptionsCopy().get(0).getType());

        assertEquals(1, queryResult.getRowsCount());
        final Cell resultCell = queryResult.iterator().next().iterator().next();

        assertNull(resultCell);
    }

    @Test
    public void TestQueryingInvalidTableNameResultsInError() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        final String queryText = "select time from GeoChicken";

        Query query = new Query.Builder(queryText).build();
        RiakFuture<QueryResult, String> future = client.executeAsync(query);
        future.await();
        assertFalse(future.isSuccess());
        assertEquals(future.cause().getClass(), RiakResponseException.class);
    }

    @Test
    public void TestStoringDataOutOfOrderResultsInError() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        Row row = new Row(com.basho.riak.client.core.query.timeseries.Cell.newTimestamp(fifteenMinsAgo), new Cell("hash1"), new Cell("user1"), new Cell("cloudy"), new Cell(79.0));
        Store store = new Store.Builder("GeoChicken").withRow(row).build();

        RiakFuture<Void, String> future = client.executeAsync(store);
        future.await();
        assertFalse(future.isSuccess());
        assertEquals(future.cause().getClass(), RiakResponseException.class);
    }

    @Test
    public void TestFetchingSingleRowsWorks() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        final List<Cell> keyCells = Arrays.asList(new Cell("hash2"), new Cell("user4"), com.basho.riak.client.core
                .query.timeseries.Cell
                .newTimestamp(fifteenMinsAgo));
        Fetch fetch = new Fetch.Builder(tableName, keyCells).build();

        QueryResult queryResult = client.execute(fetch);
        assertEquals(1, queryResult.getRowsCount());
        Row row = queryResult.getRowsCopy().get(0);
        assertEquals("rain", row.getCellsCopy().get(3).getVarcharAsUTF8String());
        assertEquals(79.0, row.getCellsCopy().get(4).getDouble(), Double.MIN_VALUE);
    }

    @Test
    public void TestFetchingWithNotFoundKeyReturnsNoRows() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        final List<Cell> keyCells = Arrays.asList(new Cell("nohash"), new Cell("nouser"), com.basho.riak.client.core.query.timeseries.Cell
                .newTimestamp(fifteenMinsAgo));
        Fetch fetch = new Fetch.Builder(tableName, keyCells).build();

        QueryResult queryResult = client.execute(fetch);
        assertEquals(0, queryResult.getRowsCount());
    }

    @Test
    public void TestDeletingRowRemovesItFromQueries() throws ExecutionException, InterruptedException
    {
        final List<Cell> keyCells = Arrays.asList(new Cell("hash2"), new Cell("user4"), com.basho.riak.client.core.query.timeseries.Cell
                .newTimestamp(fiveMinsAgo));

        RiakClient client = new RiakClient(cluster);

        // Assert we have a row
        Fetch fetch = new Fetch.Builder(tableName, keyCells).build();
        QueryResult queryResult = client.execute(fetch);
        assertEquals(1, queryResult.getRowsCount());

        // Delete row
        Delete delete = new Delete.Builder(tableName, keyCells).build();

        final RiakFuture<Void, String> deleteFuture = client.executeAsync(delete);

        deleteFuture.await();
        assertTrue(deleteFuture.isSuccess());
        assertNull(deleteFuture.cause());

        // Assert that the row is no longer with us
        Fetch fetch2 = new Fetch.Builder(tableName, keyCells).build();
        QueryResult queryResult2 = client.execute(fetch2);
        assertEquals(0, queryResult2.getRowsCount());
    }

    @Test
    public void TestDeletingWithNotFoundKeyDoesNotReturnError() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        final List<Cell> keyCells = Arrays.asList(new Cell("nohash"), new Cell("nouser"), com.basho.riak.client.core.query.timeseries.Cell
                .newTimestamp(fifteenMinsAgo));
        Delete delete = new Delete.Builder(tableName, keyCells).build();

        final RiakFuture<Void, String> deleteFuture = client.executeAsync(delete);

        deleteFuture.await();
        assertTrue(deleteFuture.isSuccess());
        assertNull(deleteFuture.cause());
    }

    @Test
    public void TestDescribeTable() throws InterruptedException, ExecutionException
    {
        RiakClient client = new RiakClient(cluster);

        Query query = new Query.Builder("DESCRIBE " + tableName).build();

        final RiakFuture<QueryResult, String> resultFuture = client.executeAsync(query);

        resultFuture.await();
        assertTrue(resultFuture.isSuccess());
        assertNull(resultFuture.cause());

        final QueryResult tableDescription = resultFuture.get();
        assertEquals(7, tableDescription.getRowsCount());
        assertEquals(5, tableDescription.getColumnDescriptionsCopy().size());
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

    private static <R1 extends Row, R2 extends Row> void assertRowMatches(R1 expected, R2 actual)
    {
        List<Cell> expectedCells = toList(expected.iterator());
        List<Cell> actualCells = toList(actual.iterator());

        assertEquals(expectedCells.get(0).getVarcharAsUTF8String(), actualCells.get(0).getVarcharAsUTF8String());
        assertEquals(expectedCells.get(1).getVarcharAsUTF8String(), actualCells.get(1).getVarcharAsUTF8String());
        assertEquals(expectedCells.get(2).getTimestamp(),           actualCells.get(2).getTimestamp());
        assertEquals(expectedCells.get(3).getVarcharAsUTF8String(), actualCells.get(3).getVarcharAsUTF8String());

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

        assertEquals(expectedCells.get(6).getBoolean(),  actualCells.get(6).getBoolean());

    }

}
