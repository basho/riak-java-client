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
import junit.framework.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 *
 * Schema for the Timeseries table we're using:
 *
 *   CREATE TABLE GeoCheckin
 *   (
 *      myfamily    varchar   not null,
 *      myseries    varchar   not null,
 *      time        timestamp not null,
 *      weather     varchar   not null,
 *      temperature float,
 *      PRIMARY KEY (
 *          (quantum(time, 15, 'm'), myfamily, myseries),
 *          time, myfamily, myseries
 *      )
 *   )
 */
public class ITestTimeSeries extends ITestBase
{
    private boolean InsertedData = false;

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

        final Calendar fifteenMinsAgo = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        fifteenMinsAgo.add(Calendar.MINUTE, -15);
        final Calendar fiveMinsAgo = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        fiveMinsAgo.add(Calendar.MINUTE, -5);
        final Calendar now = new GregorianCalendar(TimeZone.getTimeZone("UTC"));

        final List<Row> rows = Arrays.asList(
                new Row(new Cell("family1"), new Cell("series1"), new Cell(fifteenMinsAgo), new Cell("cloudy"), new Cell(79.0)),
                new Row(new Cell("family1"), new Cell("series1"), new Cell(fiveMinsAgo),    new Cell("sunny"),  new Cell(80.5)),
                new Row(new Cell("family1"), new Cell("series2"), new Cell(now),            new Cell("sunny"),  new Cell(81.0)));

        Store store = new Store.Builder(tableName).withRows(rows).build();

        System.out.println(fifteenMinsAgo.getTimeInMillis());
        System.out.println(now.getTimeInMillis());

        RiakFuture<Void, BinaryValue> execFuture = client.executeAsync(store);

        execFuture.await();
        Throwable cause = execFuture.cause();
        assertEquals(cause.getMessage(), true, execFuture.isSuccess());
        InsertedData = true;
    }

    @Test
    public void QueryingData() throws ExecutionException, InterruptedException
    {
        Assert.assertTrue("No data inserted, skipping test", InsertedData);

        RiakClient client = new RiakClient(cluster);

        final Calendar sixteenMinutesAgo = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        sixteenMinutesAgo.add(Calendar.MINUTE, -16);
        final Calendar now = new GregorianCalendar(TimeZone.getTimeZone("UTC"));

        final String queryText = "select weather from GeoCheckin " +
                "where myseries = \"series1\" and " +
                "(time > " + sixteenMinutesAgo.getTimeInMillis() +" and (time < "+ now.getTimeInMillis() + "))";

        System.out.println(queryText);

        Query query = new Query.Builder(queryText).build();
        QueryResult queryResult = client.execute(query);

        assertEquals(1, queryResult.getColumnDescriptions().size());
        assertEquals(2, queryResult.getRows().size());
    }

}
