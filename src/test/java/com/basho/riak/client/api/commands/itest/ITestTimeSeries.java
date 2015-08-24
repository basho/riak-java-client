package com.basho.riak.client.api.commands.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Query;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.TimeSeriesQueryOperation;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.client.core.util.BinaryValue;
import com.sun.javaws.exceptions.InvalidArgumentException;
import org.junit.Test;

import java.util.List;
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
    public void creatingCollections() throws ExecutionException, InterruptedException
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
}
