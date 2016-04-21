package com.basho.riak.client.core.operations.itest.ts;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.CreateTable;
import com.basho.riak.client.api.commands.timeseries.Query;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.netty.RiakResponseException;
import com.basho.riak.client.core.operations.FetchBucketPropsOperation;
import com.basho.riak.client.core.operations.itest.ITestAutoCleanupBase;
import com.basho.riak.client.core.operations.ts.QueryOperation;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.timeseries.*;
import org.junit.Assume;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


/**
 * Time Series Operation Integration Tests Base
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
 *
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public abstract class ITestTsBase extends ITestAutoCleanupBase
{
    protected final static String tableName = "GeoCheckin_Wide";

    protected final static TableDefinition GeoCheckinWideTableDefinition = new TableDefinition(tableName,
        Arrays.asList(
            new FullColumnDescription("geohash", ColumnDescription.ColumnType.VARCHAR,  false, 1),
            new FullColumnDescription("user", ColumnDescription.ColumnType.VARCHAR,  false, 2),
            new FullColumnDescription("time", ColumnDescription.ColumnType.TIMESTAMP,  false, 3),
            new FullColumnDescription("weather", ColumnDescription.ColumnType.VARCHAR,  false),
            new FullColumnDescription("temperature", ColumnDescription.ColumnType.DOUBLE, true),
            new FullColumnDescription("uv_index", ColumnDescription.ColumnType.SINT64,  true),
            new FullColumnDescription("observed", ColumnDescription.ColumnType.BOOLEAN,  false)
        )
    );

    protected final static long now = 1443806900000L; // "now"
    protected final static long fiveMinsInMS = 5L * 60L * 1000L;
    protected final static long fiveMinsAgo = now - fiveMinsInMS;
    protected final static long tenMinsAgo = fiveMinsAgo - fiveMinsInMS;
    protected final static long fifteenMinsAgo = tenMinsAgo - fiveMinsInMS;
    protected final static long fifteenMinsInFuture = now + (fiveMinsInMS * 3L);


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

    @BeforeClass
    public static void BeforeClass() throws ExecutionException, InterruptedException {
        Assume.assumeTrue(testTimeSeries);
        final RiakClient client = new RiakClient(cluster);
        createTableIfNotExists(client, GeoCheckinWideTableDefinition);
    }

    protected static boolean isTableExistence(RiakClient client, String tableName) throws InterruptedException, ExecutionException {
        final Namespace ns = new Namespace(tableName, tableName);

        final FetchBucketPropsOperation fetchProps = new FetchBucketPropsOperation.Builder(ns).build();

        client.getRiakCluster().execute(fetchProps);

        try
        {
            fetchProps.get().getBucketProperties();
            return true;
        }
        catch (ExecutionException ex)
        {
            if (ex.getCause() instanceof RiakResponseException
                    && ex.getCause().getMessage().startsWith("No bucket-type named"))
            {
                return false;
            }

            throw ex;
        }
    }

    protected static RiakFuture<Void, String> createTableAsync(final RiakClient client, TableDefinition tableDefinition) throws InterruptedException {
        final CreateTable cmd = new CreateTable.Builder(tableDefinition)
                // TODO: avoid usage of hardcoded quanta
                .withQuantum(15, TimeUnit.MINUTES)
                .build();

        return client.executeAsync(cmd);
    }

    protected static void createTable(final RiakClient client, TableDefinition tableDefinition) throws InterruptedException, ExecutionException {
        createTableAsync(client, tableDefinition).get();
    }

    protected static void createTableIfNotExists(final RiakClient client, TableDefinition tableDefinition) throws InterruptedException, ExecutionException {
        if (!isTableExistence(client, tableDefinition.getTableName()))
        {
            createTableAsync(client, tableDefinition).get();
        }
    }

    protected static QueryResult executeQuery(QueryOperation.Builder builder) throws ExecutionException, InterruptedException
    {
        final RiakFuture<QueryResult, String> future = cluster.execute(builder.build());

        future.await();

        assertFutureSuccess(future);
        return future.get();
    }

    protected static QueryResult executeQuery(Query.Builder builder) throws ExecutionException, InterruptedException
    {
        final RiakClient client = new RiakClient(cluster);
        final RiakFuture<QueryResult, String> future = client.executeAsync(builder.build());

        future.await();

        assertFutureSuccess(future);
        return future.get();
    }
}
