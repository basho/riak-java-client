package com.basho.riak.client.api.commands.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Query;
import com.basho.riak.client.api.commands.timeseries.Store;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.TimeSeriesQueryOperation;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.client.core.util.BinaryValue;
import com.sun.javaws.exceptions.InvalidArgumentException;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class ITestTimeSeries extends ITestBase
{
    @Test
    public void creatingCollections()
    {
        try
        {
            RiakClient client = new RiakClient(cluster);

            final String queryText =
                    "create table time_series (" +
                            "time timestamp not null," +
                            "user_id varchar not null," +
                            "temperature_k float," +
                            "primary key (time))";

            Query query = new Query.Builder(queryText).build();

            QueryResult queryResult = client.execute(query);

            // These will change, putting test here to demonstrate that we are converting the types correctly.
            assertEquals(1, queryResult.getColumnDescriptions().size());
            assertEquals("asdf", queryResult.getColumnDescriptions().get(0).getName());

            List<Row> rows = queryResult.getRows();
            assertEquals(1, rows.size());
            List<Cell> cells = rows.get(0).getCells();
            assertEquals(1, cells.size());
            Cell cell = cells.get(0);
            assertTrue(cell.hasString());
            assertEquals("jkl;", cell.getUtf8String());
        }
        catch (ExecutionException ex)
        {
            System.out.println(ex.getCause().getCause());
        }
        catch (InterruptedException ex)
        {
            Logger.getLogger(ITestFetchValue.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void ensureWeCatchInvalidParams()
    {
        final String queryText =
                "create table time_series (" +
                        "time timestamp not null," +
                        "user_id varchar not null," +
                        "temperature_k float," +
                        "primary key (time))";

        Query.Builder query = new Query.Builder(queryText);
        query.addStringParameter("foo", "123");
    }


    @Test
    public void StoringData() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        final String tableName = "time_series";

        final Calendar date1 = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        date1.add(Calendar.MILLISECOND, -10);
        final Calendar date2 = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        date1.add(Calendar.MILLISECOND, -5);
        final Calendar date3 = new GregorianCalendar(TimeZone.getTimeZone("UTC"));

        final List<Row> rows = Arrays.asList(new Row(new Cell(date1), new Cell("bryce"), new Cell(305.37)),

                                             new Row(new Cell(date2), new Cell("bryce"), new Cell(300.12)),

                                             new Row(new Cell(date3), new Cell("bryce"), new Cell(295.95)));

        Store store = new Store.Builder(tableName).withRows(rows).build();

        RiakFuture<Void, BinaryValue> execFuture = client.executeAsync(store);

        execFuture.await();
        assertTrue(execFuture.isSuccess());
    }

}
