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
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.TableDefinition;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Time Series Fetch Bug Reproducer
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ITestTimeSeriesFetchBug extends ITestTsBase
{
    private final static String tableName = "foo" + new Random().nextInt(Integer.MAX_VALUE);

    private RiakFuture<Void, String> createTableAsync(final RiakClient client, String tableName)
            throws InterruptedException
    {
        final TableDefinition tableDef = new TableDefinition(tableName,
                                                             GeoCheckinWideTableDefinition.getFullColumnDescriptions());

        return createTableAsync(client, tableDef);
    }

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
        final RiakFuture<FetchBucketPropsOperation.Response, Namespace> getBucketPropsFuture = client.executeAsync(
                fetchBucketPropsCmd);

        getBucketPropsFuture.await();
        assertFutureSuccess(getBucketPropsFuture);
        assertTrue(1 == getBucketPropsFuture.get().getBucketProperties().getNVal());
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
        assertTrue(queryResult.getRowsCount() == rows.size());
    }

    @Test
    public void test_n_TestDeletingRowRemovesItFromQueries() throws ExecutionException, InterruptedException
    {
        boolean anyFailures = false;
        System.out.println("riakc_ts:get(Pid, <<\"" + tableName + "\">>, [<<\"hash2\">>, <<\"user4\">>, " +
                                   "1443806600000], []).");

        final List<Cell> keyCells = Arrays.asList(new Cell("hash2"), new Cell("user4"), Cell.newTimestamp(fiveMinsAgo));

        RiakClient client = new RiakClient(cluster);

        // See if we can fetch the row via a Fetch operation
        try
        {
            Fetch fetch = new Fetch.Builder(tableName, keyCells).build();
            QueryResult fetchResult = client.execute(fetch);
            assertEquals(1, fetchResult.getRowsCount());
        }
        catch (ExecutionException ex)
        {
            PrintException(ex);
            anyFailures = true;
        }

        // Delete row
        Delete delete = new Delete.Builder(tableName, keyCells).build();

        final RiakFuture<Void, String> deleteFuture = client.executeAsync(delete);

        deleteFuture.await();
        if (deleteFuture.isSuccess())
        {
            System.out.println("Delete success");
        }
        else
        {
            System.out.println("Delete failure");
            anyFailures = true;
        }

        // Assert that the row is no longer with us
        while (true)
        {
            boolean fetchSuccess = false;
            try
            {
                Fetch fetch2 = new Fetch.Builder(tableName, keyCells).build();
                QueryResult postDeleteFetchResult = client.execute(fetch2);
                assertEquals(0, postDeleteFetchResult.getRowsCount());

                System.out.println("Fetch success");
                fetchSuccess = true;
            }
            catch (ExecutionException ex)
            {
                PrintException(ex);
                anyFailures = true;
            }

            try
            {
                final String queryText = "select * from " + tableName + " " + "where geohash = '" +
                        keyCells.get(0).getVarcharAsUTF8String() + "' and " + "user = '" +
                        keyCells.get(1).getVarcharAsUTF8String() + "' and " + "(time > " +
                        (keyCells.get(2).getTimestamp() - 1) + " and " + "(time < " +
                        (keyCells.get(2).getTimestamp() + 1) + ")) ";

                // See if we can fetch the row via a Query operation
                final QueryResult queryResult = executeQuery(new Query.Builder(queryText));
                if (queryResult.getRowsCount() != 0)
                {
                    System.out.println("Query Failure, found " + queryResult.getRowsCount() + " rows");
                    continue;
                }

                System.out.println("Query success");
                if (fetchSuccess)
                {
                    break;
                }
            }
            catch (ExecutionException ex)
            {
                PrintException(ex);
                anyFailures = true;
            }

        }

        assertFalse(anyFailures);
    }

    private void PrintException(ExecutionException ex)
    {
        RiakResponseException e = (RiakResponseException) ex.getCause();
        System.out.println("Fetch failure: " + e.toString() + "(" + e.getCode() + ")" + ": " + e.getMessage());
    }
}
