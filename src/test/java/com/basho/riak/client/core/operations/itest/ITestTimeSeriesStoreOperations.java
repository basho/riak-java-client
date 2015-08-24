package com.basho.riak.client.core.operations.itest;

import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.TimeSeriesQueryOperation;
import com.basho.riak.client.core.operations.TimeSeriesStoreOperation;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */

public class ITestTimeSeriesStoreOperations extends ITestBase
{

    @Test
    public void writesDataWithoutError() throws ExecutionException, InterruptedException
    {
        final String tableName = "time_series";
        final BinaryValue tableNameBV = BinaryValue.create(tableName);
        final Calendar date1 = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        date1.add(Calendar.MILLISECOND, -10);
        final Calendar date2 = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        date1.add(Calendar.MILLISECOND, -5);
        final Calendar date3 = new GregorianCalendar(TimeZone.getTimeZone("UTC"));

        final List<Row> rows = Arrays.asList(
                new Row(new Cell(date1),
                        new Cell("bryce"),
                        new Cell(305.37)),

                new Row(new Cell(date2),
                        new Cell("bryce"),
                        new Cell(300.12)),

                new Row(new Cell(date3),
                        new Cell("bryce"),
                        new Cell(295.95)));

        TimeSeriesStoreOperation storeOp = new TimeSeriesStoreOperation.Builder(tableNameBV).withRows(rows).build();
        RiakFuture<Void, BinaryValue> future = cluster.execute(storeOp);

        future.get();
        assertTrue(future.isSuccess());
    }

}
