package com.basho.riak.client.api.commands.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Fetch;
import com.basho.riak.client.api.commands.timeseries.ListKeys;
import com.basho.riak.client.api.commands.timeseries.Query;
import com.basho.riak.client.api.commands.timeseries.Store;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.itest.ts.ITestTsBase;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ITestTimeSeriesBlobListKeys extends ITestTsBase
{
    private static String TableName = "BLURB";
    private static String CreateTable =
            "CREATE TABLE BLURB (blurb blob not null, time timestamp not null, PRIMARY KEY((blurb, quantum(time, 15, 'm')), blurb, time))";

    @Test
    public void a_CreateTable() throws ExecutionException, InterruptedException
    {
        final RiakClient client = new RiakClient(cluster);

        final Query createTableQuery = new Query.Builder(CreateTable).build();
        final RiakFuture<QueryResult, String> createTableFuture = client.executeAsync(createTableQuery);

        createTableFuture.await();
        assertTrue(createTableFuture.isSuccess());
    }

    @Test
    public void b_PopulateTable() throws InterruptedException
    {
        final RiakClient client = new RiakClient(cluster);

        final Row row = new Row(new Cell(new byte[] {0, 1, 2, 3, 4, 5, 6, 7}), Cell.newTimestamp(now));
        final Store storeCommand = new Store.Builder(TableName).withRow(row).build();

        final RiakFuture<Void, String> storeFuture = client.executeAsync(storeCommand);

        storeFuture.await();
        assertTrue(storeFuture.isSuccess());
    }


    @Test
    public void c_ListKeysAndFetch() throws InterruptedException, ExecutionException
    {
        final RiakClient client = new RiakClient(cluster);

        final ListKeys listKeysCommand = new ListKeys.Builder(TableName).build();

        final RiakFuture<QueryResult, String> listKeysFuture = client.executeAsync(listKeysCommand);

        listKeysFuture.await();
        assertTrue(listKeysFuture.isSuccess());
        final QueryResult queryResult = listKeysFuture.get();

        final List<Row> rows = queryResult.getRowsCopy();
        assertEquals(1, rows.size());

        final Row row = rows.get(0);
        assertEquals(2, row.getCellsCount());

        final List<Cell> cells = row.getCellsCopy();

        assertTrue(cells.get(0).hasVarcharValue());
        assertFalse(cells.get(0).hasBlob());
        assertTrue(cells.get(1).hasTimestamp());

        FetchRow(client, cells);
    }

    private void FetchRow(RiakClient client, List<Cell> cells) throws InterruptedException, ExecutionException
    {
        Fetch fetchCommand = new Fetch.Builder(TableName, cells).build();

        final RiakFuture<QueryResult, String> fetchFuture = client.executeAsync(fetchCommand);

        fetchFuture.await();
        assertTrue(fetchFuture.isSuccess());

        final QueryResult fetchResult = fetchFuture.get();
        assertEquals(1, fetchResult.getRowsCount());

        final List<Row> fetchRows = fetchResult.getRowsCopy();
        assertEquals(1, fetchRows.size());

        final Row fetchRow = fetchRows.get(0);
        assertEquals(2, fetchRow.getCellsCount());

        final List<Cell> fetchCells = fetchRow.getCellsCopy();

        assertFalse(fetchCells.get(0).hasVarcharValue());
        assertTrue(fetchCells.get(0).hasBlob());
        assertTrue(fetchCells.get(1).hasTimestamp());
    }
}
