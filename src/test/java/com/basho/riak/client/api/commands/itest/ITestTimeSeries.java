package com.basho.riak.client.api.commands.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.buckets.FetchBucketProperties;
import com.basho.riak.client.api.commands.buckets.StoreBucketProperties;
import com.basho.riak.client.api.commands.timeseries.*;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.FetchBucketPropsOperation;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.core.operations.itest.ts.ITestTsBase;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.timeseries.*;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runners.MethodSorters;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

/**
 * Time Series Commands Integration Tests
 *
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 *
 * Schema for the Timeseries table we're using:
 *
 *   CREATE TABLE GeoCheckin&lt;RandomInteger&gt;
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
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ITestTimeSeries extends ITestTsBase
{
    private final static String tableName = "GeoHash" + new Random().nextInt(Integer.MAX_VALUE);
    private static final String BAD_TABLE_NAME = "GeoChicken";

    private RiakFuture<Void, String> createTableAsync(final RiakClient client, String tableName) throws InterruptedException {
        final TableDefinition tableDef = new TableDefinition(tableName, GeoCheckinWideTableDefinition.getFullColumnDescriptions());

        return createTableAsync(client, tableDef);
    }

    @Rule
    public ExpectedException thrown= ExpectedException.none();

    @Test
    public void test_a_TestCreateTableAndChangeNVal() throws InterruptedException, ExecutionException
    {
        final RiakClient client = new RiakClient(cluster);
        final RiakFuture<Void, String> resultFuture = createTableAsync(client, tableName);
        resultFuture.await();
        assertFutureSuccess(resultFuture);


        final Namespace namespace = new Namespace(tableName, tableName);
        StoreBucketProperties storeBucketPropsCmd = new StoreBucketProperties.Builder(namespace).withNVal(1).build();
        final RiakFuture<Void, Namespace> storeBucketPropsFuture = client.executeAsync(storeBucketPropsCmd);

        storeBucketPropsFuture.await();
        assertFutureSuccess(storeBucketPropsFuture);

        FetchBucketProperties fetchBucketPropsCmd = new FetchBucketProperties.Builder(namespace).build();
        final RiakFuture<FetchBucketPropsOperation.Response, Namespace> getBucketPropsFuture =
                client.executeAsync(fetchBucketPropsCmd);

        getBucketPropsFuture.await();
        assertFutureSuccess(getBucketPropsFuture);
        assertTrue(1 == getBucketPropsFuture.get().getBucketProperties().getNVal());
    }

    @Test
    public void test_b_TestCreateBadTable() throws InterruptedException
    {
        final RiakClient client = new RiakClient(cluster);
        final RiakFuture<Void, String> resultFuture = createTableAsync(client, tableName);

        resultFuture.await();
        assertFutureFailure(resultFuture);
    }

    @Test
    public void test_c_StoringData() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        Store store = new Store.Builder(tableName).withRows(rows).build();

        RiakFuture<Void, String> execFuture = client.executeAsync(store);

        execFuture.await();
        assertFutureSuccess(execFuture);
    }

    @Test
    public void test_d_TestListingKeysReturnsThem() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        ListKeys listKeys = new ListKeys.Builder(tableName).build();

        final RiakFuture<QueryResult, String> listKeysFuture = client.executeAsync(listKeys);

        listKeysFuture.await();
        assertFutureSuccess(listKeysFuture);

        final QueryResult queryResult = listKeysFuture.get();
        assertTrue(queryResult.getRowsCount() > 0);
    }

    @Test
    public void test_e_QueryingDataNoMatches() throws ExecutionException, InterruptedException
    {
        final String queryText = "select * from " + tableName + " Where time > 1 and time < 10 and user='user1' and geohash='hash1'";
        final QueryResult queryResult = executeQuery(new Query.Builder(queryText));

        assertNotNull(queryResult);
        assertEquals(0, queryResult.getColumnDescriptionsCopy().size());
        assertEquals(0, queryResult.getRowsCount());
    }

    @Test
    public void test_f_QueryingDataWithMinimumPredicate() throws ExecutionException, InterruptedException
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
    public void test_g_QueryingDataWithExtraPredicate() throws ExecutionException, InterruptedException
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
    public void test_h_QueryingDataAcrossManyQuantum() throws ExecutionException, InterruptedException
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
    public void test_i_TestThatNullsAreSavedAndFetchedCorrectly() throws ExecutionException, InterruptedException
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
    public void test_j_TestQueryingInvalidTableNameResultsInError() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        final String queryText = "select time from GeoChicken";

        Query query = new Query.Builder(queryText).build();
        RiakFuture<QueryResult, String> future = client.executeAsync(query);

        future.await();
        assertFutureFailure(future);
    }

    @Test
    public void test_k_TestStoringDataOutOfOrderResultsInError() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        Row row = new Row(com.basho.riak.client.core.query.timeseries.Cell.newTimestamp(fifteenMinsAgo), new Cell("hash1"), new Cell("user1"), new Cell("cloudy"), new Cell(79.0));
        Store store = new Store.Builder(BAD_TABLE_NAME).withRow(row).build();

        RiakFuture<Void, String> future = client.executeAsync(store);

        future.await();
        assertFutureFailure(future);
    }

    @Test
    public void test_l_TestFetchingSingleRowsWorks() throws ExecutionException, InterruptedException
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
    public void test_m_TestFetchingWithNotFoundKeyReturnsNoRows() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        final List<Cell> keyCells = Arrays.asList(new Cell("nohash"), new Cell("nouser"), com.basho.riak.client.core.query.timeseries.Cell
                .newTimestamp(fifteenMinsAgo));
        Fetch fetch = new Fetch.Builder(tableName, keyCells).build();

        QueryResult queryResult = client.execute(fetch);
        assertEquals(0, queryResult.getRowsCount());
    }

    @Test
    public void test_n_TestDeletingRowRemovesItFromQueries() throws ExecutionException, InterruptedException
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
        assertFutureSuccess(deleteFuture);

        // Assert that the row is no longer with us
        Fetch fetch2 = new Fetch.Builder(tableName, keyCells).build();
        QueryResult queryResult2 = client.execute(fetch2);
        assertEquals(0, queryResult2.getRowsCount());
    }

    @Test
    public void test_o_TestDeletingWithNotFoundKeyDoesNotReturnError() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        final List<Cell> keyCells = Arrays.asList(new Cell("nohash"), new Cell("nouser"), com.basho.riak.client.core.query.timeseries.Cell
                .newTimestamp(fifteenMinsAgo));
        Delete delete = new Delete.Builder(tableName, keyCells).build();

        final RiakFuture<Void, String> deleteFuture = client.executeAsync(delete);

        deleteFuture.await();
        assertFutureFailure(deleteFuture);
    }

    @Test
    public void test_p_TestDescribeTable() throws InterruptedException, ExecutionException
    {
        RiakClient client = new RiakClient(cluster);

        Query query = new Query.Builder("DESCRIBE " + tableName).build();

        final RiakFuture<QueryResult, String> resultFuture = client.executeAsync(query);

        resultFuture.await();
        assertFutureSuccess(resultFuture);

        final QueryResult tableDescription = resultFuture.get();
        assertEquals(7, tableDescription.getRowsCount());
        assertEquals(5, tableDescription.getColumnDescriptionsCopy().size());
    }

    @Test
    public void test_q_TestDescribeTableCommand() throws InterruptedException, ExecutionException
    {
        RiakClient client = new RiakClient(cluster);

        DescribeTable describe = new DescribeTable(tableName);

        final RiakFuture<TableDefinition, String> describeFuture = client.executeAsync(describe);

        describeFuture.await();
        assertFutureSuccess(describeFuture);

        final TableDefinition tableDefinition = describeFuture.get();
        final Collection<FullColumnDescription> fullColumnDescriptions = tableDefinition.getFullColumnDescriptions();
        assertEquals(7, fullColumnDescriptions.size());

        TableDefinitionTest.assertFullColumnDefinitionsMatch(GetCreatedTableFullDescriptions(),
                new ArrayList<FullColumnDescription>(fullColumnDescriptions));
    }

    @Test
    public void test_r_TestDescribeTableCommandForNonExistingTable() throws InterruptedException, ExecutionException
    {
        RiakClient client = new RiakClient(cluster);

        DescribeTable describe = new DescribeTable(BAD_TABLE_NAME);

        final RiakFuture<TableDefinition, String> describeFuture = client.executeAsync(describe);

        describeFuture.await();
        assertFutureFailure(describeFuture);

        final String message = describeFuture.cause().getMessage();
        assertTrue(message.toLowerCase().contains(BAD_TABLE_NAME.toLowerCase()));
        assertTrue(message.toLowerCase().contains("not an active table"));
    }

    @Test
    public void test_z_TestPBCErrorsReturnWhenSecurityIsOn() throws InterruptedException, ExecutionException
    {
        assumeTrue(security);

        thrown.expect(ExecutionException.class);
        thrown.expectMessage("Security is enabled, please STARTTLS first");

        // Build connection WITHOUT security
        final RiakNode node = new RiakNode.Builder().withRemoteAddress(hostname).withRemotePort(pbcPort).build();
        final RiakCluster cluster = new RiakCluster.Builder(node).build();
        cluster.start();
        final RiakClient client = new RiakClient(cluster);

        Query query = new Query.Builder("DESCRIBE " + tableName).build();

        final QueryResult result = client.execute(query);
    }

    private static List<FullColumnDescription> GetCreatedTableFullDescriptions()
    {
        return Arrays.asList(new FullColumnDescription("geohash", ColumnDescription.ColumnType.VARCHAR, false, 1),
                             new FullColumnDescription("user", ColumnDescription.ColumnType.VARCHAR, false, 2),
                             new FullColumnDescription("time", ColumnDescription.ColumnType.TIMESTAMP, false, 3),
                             new FullColumnDescription("weather", ColumnDescription.ColumnType.VARCHAR, false),
                             new FullColumnDescription("temperature", ColumnDescription.ColumnType.DOUBLE, true),
                             new FullColumnDescription("uv_index", ColumnDescription.ColumnType.SINT64, true),
                             new FullColumnDescription("observed", ColumnDescription.ColumnType.BOOLEAN, false));
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
