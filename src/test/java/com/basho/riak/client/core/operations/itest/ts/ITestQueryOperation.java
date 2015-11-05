package com.basho.riak.client.core.operations.itest.ts;

import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.ts.QueryOperation;
import com.basho.riak.client.core.operations.ts.StoreOperation;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.util.BinaryValue;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * Time Series Query Operation Integration Tests
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */

public class ITestQueryOperation extends ITestTsBase
{
    @BeforeClass
    public static void InsertData() throws ExecutionException, InterruptedException
    {
        final BinaryValue tableNameBV = BinaryValue.create(tableName);

        StoreOperation storeOp = new StoreOperation.Builder(tableNameBV).withRows(rows).build();
        RiakFuture<Void, BinaryValue> future = cluster.execute(storeOp);

        future.get();
        assertTrue(future.isSuccess());
    }

    @Test
    public void queryNoMatches() throws ExecutionException, InterruptedException
    {
        final String queryText = "select * from GeoCheckin " +
                                 "where time > 0 " +
                                 "  and time < 10 " +
                                 "  and user ='user1'" +
                                 "  and geohash ='hash1'";

        final BinaryValue queryTextBS = BinaryValue.create(queryText);

        QueryOperation queryOp = new QueryOperation.Builder(queryTextBS).build();
        RiakFuture<QueryResult, BinaryValue> future = cluster.execute(queryOp);

        QueryResult queryResult = future.get();
        assertTrue(future.isSuccess());

        assertNotNull(queryResult);
        assertEquals(0, queryResult.getColumnDescriptions().size());
        assertEquals(0, queryResult.getRows().size());
    }

    @Test
    public void querySomeMatches() throws ExecutionException, InterruptedException
    {
        final String queryText = "select * from GeoCheckin " +
                                 "where time > " + tenMinsAgo +
                                 "  and time < "+ now +
                                 "  and user ='user2'" +
                                 "  and geohash ='hash1'";
        final BinaryValue queryTextBS = BinaryValue.create(queryText);

        QueryOperation queryOp = new QueryOperation.Builder(queryTextBS).build();
        RiakFuture<QueryResult, BinaryValue> future = cluster.execute(queryOp);

        QueryResult queryResult = future.get();
        assertTrue(future.isSuccess());

        assertNotNull(queryResult);
        assertEquals(7, queryResult.getColumnDescriptions().size());
        assertEquals(1, queryResult.getRows().size());
    }
}
