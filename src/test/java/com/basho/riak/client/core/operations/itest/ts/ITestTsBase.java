package com.basho.riak.client.core.operations.itest.ts;

import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Assume;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.List;

/**
 * Time Series Operation Integration Tests Base
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
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public abstract class ITestTsBase extends ITestBase
{
    protected final static String tableName = "GeoCheckin";
    protected final static BinaryValue tableNameBV = BinaryValue.createFromUtf8(tableName);

    protected final static long now = 1443806900000l; // "now"
    protected final static long fiveMinsInMS = 5l * 60l * 1000l;
    protected final static long fiveMinsAgo = now - fiveMinsInMS;
    protected final static long tenMinsAgo = fiveMinsAgo - fiveMinsInMS;
    protected final static long fifteenMinsAgo = tenMinsAgo - fiveMinsInMS;

    protected final static List<Row> rows = Arrays.asList(
            // Geohash, User, timestamp, weather, temperature, UV Index
            new Row(new Cell("hash1"), new Cell("user2"), Cell.newTimestamp(fifteenMinsAgo), new Cell("rain"), new Cell(79.0), new Cell(1)),
            new Row(new Cell("hash1"), new Cell("user2"), Cell.newTimestamp(fiveMinsAgo), new Cell("wind"),  new Cell(50.5), new Cell(2)),
            new Row(new Cell("hash1"), new Cell("user2"), Cell.newTimestamp(now), new Cell("snow"),  new Cell(20.0), new Cell(10)),
            new Row(new Cell("hash2"), new Cell("user4"), Cell.newTimestamp(fifteenMinsAgo), new Cell("rain"), new Cell(79.0), new Cell(2)),
            new Row(new Cell("hash2"), new Cell("user4"), Cell.newTimestamp(fiveMinsAgo), new Cell("wind"),  new Cell(50.5), new Cell(3)),
            new Row(new Cell("hash2"), new Cell("user4"), Cell.newTimestamp(now), new Cell("snow"),  new Cell(20.0), new Cell(11)));

    //@BeforeClass
    //public static void BeforeClass()
    //{
    //    Assume.assumeTrue(testTimeSeries);
    //}
}
