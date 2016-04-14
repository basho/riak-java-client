package com.basho.riak.client.core.netty;

import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.ts.FetchOperation;
import com.basho.riak.client.core.operations.ts.QueryOperation;
import com.basho.riak.client.core.operations.ts.StoreOperation;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 */
public class ITestTTBNode {

    protected final static String tableName = "GeoCheckin";

    protected final static long now = 1443806900000l; // "now"
    protected final static long fiveMinsInMS = 5l * 60l * 1000l;
    protected final static long fiveMinsAgo = now - fiveMinsInMS;
    protected final static long tenMinsAgo = fiveMinsAgo - fiveMinsInMS;
    protected final static long fifteenMinsAgo = tenMinsAgo - fiveMinsInMS;
    protected final static long fifteenMinsInFuture = now + (fiveMinsInMS * 3l);

    private RiakNode riakNode;
    protected final static List<Row> rows = Arrays.asList(
            // "Normal" Data
            new Row(new Cell("hash1"), new Cell("user1"), Cell.newTimestamp(fifteenMinsAgo), new Cell("cloudy"), new Cell(79.0), new Cell(1), new Cell(true)),
            new Row(new Cell("hash1"), new Cell("user1"), Cell.newTimestamp(fiveMinsAgo), new Cell("sunny"),  new Cell(80.5), new Cell(2), new Cell(true)),
            new Row(new Cell("hash1"), new Cell("user1"), Cell.newTimestamp(now), new Cell("sunny"),  new Cell(81.0), new Cell(10), new Cell(false)),

            // Null Cell row
            new Row(new Cell("hash1"), new Cell("user2"), Cell.newTimestamp(fiveMinsAgo), new Cell("cloudy"), null, null, new Cell(true)),

            // Data for single reads / deletes
            new Row(new Cell("hash2"), new Cell("user4"), Cell.newTimestamp(fifteenMinsAgo), new Cell("rain"), new Cell(79.0), new Cell(2), new Cell(false)),
            new Row(new Cell("hash2"), new Cell("user4"), Cell.newTimestamp(fiveMinsAgo), new Cell("wind"),  new Cell(50.5), new Cell(3), new Cell(true)),
            new Row(new Cell("hash2"), new Cell("user4"), Cell.newTimestamp(now), new Cell("snow"),  new Cell(20.0), new Cell(11), new Cell(true)));


    @Before
    public void setup() throws UnknownHostException, ExecutionException, InterruptedException {
        /**
         * Riak PBC port
         *
         * In case you want/need to use a custom PBC port you may pass it by using the following system property
         */
        final int testRiakPort = Integer.getInteger("com.basho.riak.pbcport", RiakNode.Builder.DEFAULT_REMOTE_PORT);


        riakNode = new RiakNode.Builder()
                .withRemotePort(testRiakPort)
//                .withMaxConnections(1)
//                .withMinConnections(1)
                .build();

        riakNode.start();

        /*
            Since TTB is toggled in preventive manner for each conection there is no needs to toggle it manually
          */
        //final ToggleTTBEncodingOperation op = new ToggleTTBEncodingOperation(true);
        //riakNode.execute(op);
        //assertTrue(op.get().isUseNativeEncoding());
    }


    @After
    public void shutdonw(){
        if (riakNode != null) {
            riakNode.shutdown();
        }
    }

    @Test
    public void storeData() throws InterruptedException, ExecutionException {
        storeDataInternal(tableName, rows.subList(0,1));
    }

    @Test
    public void getData() throws ExecutionException, InterruptedException {
        storeDataInternal(tableName, rows);

        for (Row r: rows){
            final FetchOperation fetch = new FetchOperation.Builder(tableName,
                    r.getCellsCopy().subList(0,3) // use the only PK values
                ).build();

            QueryResult queryResult = execute(fetch);

            assertEquals(1, queryResult.getRowsCount());
            //assertArrayEquals(r.getCells().toArray(), queryResult.getRows().get(0).getCells().toArray());
        }
    }

    @Ignore
    @Test
    public void queryData() throws InterruptedException, ExecutionException {
        storeDataInternal(tableName, rows);

        final String queryText = "select * from GeoCheckin " +
                "where user = 'user1' and " +
                "geohash = 'hash1' and " +
                "(time = " + tenMinsAgo +" and " +
                "(time < "+ now + ")) ";

        final QueryOperation query = new QueryOperation.Builder(queryText).build();
        final QueryResult queryResult = execute(query);

        assertEquals(7, queryResult.getColumnDescriptionsCopy().size());
        assertEquals(1, queryResult.getRowsCount());
    }

    @Test
    public void storeDatatoNoneExistentTable() throws InterruptedException, ExecutionException {
        try {
            storeDataInternal(UUID.randomUUID().toString(), rows);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RiakResponseException
                    && e.getCause().getMessage().startsWith("Time Series table")
                    && e.getCause().getMessage().endsWith("does not exist.")) {
                // It's OK
                return;
            }
            throw e;
        }
    }

    private <T, U, S, O extends FutureOperation<T,U,S>> T execute(O operation) throws ExecutionException, InterruptedException
    {
        riakNode.execute((FutureOperation) operation);
        return (T)((FutureOperation) operation).get();
    }

    private void storeDataInternal(String table, List<Row> rows) throws ExecutionException, InterruptedException {
        final StoreOperation store = new StoreOperation.Builder(table).withRows(rows).build();

        execute(store);
    }
}
