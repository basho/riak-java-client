package com.basho.riak.client.core.operations.itest.ts;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.CoveragePlan;
import com.basho.riak.client.api.commands.timeseries.Query;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.ts.StoreOperation;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.CoverageEntry;
import com.basho.riak.client.core.query.timeseries.CoveragePlanResult;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.client.core.util.HostAndPort;

public class ITestCoveragePlan extends ITestTsBase {
    private final static Logger logger = LoggerFactory.getLogger(ITestCoveragePlan.class);
    private static long quantum = 900000L; // 15 minutes
    private static long period = 10 * quantum;
    private static long numberOfQuanta = period / quantum + 1;
    private static long from = 144806900000L;
    private static long to = from + period;

    @BeforeClass
    public static void createData() throws ExecutionException, InterruptedException {
        List<Row> rows1 = new ArrayList<Row>();
        for (long time = from; time <= to; time += quantum) {
            rows1.add(new Row(new Cell("hash1"), new Cell("user1"), Cell.newTimestamp(time), new Cell("cloudy"), new Cell(
                    79.0), new Cell(1), new Cell(true)));
        }
        StoreOperation storeOp = new StoreOperation.Builder(tableName).withRows(rows1).build();
        RiakFuture<Void, String> future = cluster.execute(storeOp);

        future.get();
    }

    @Test
    public void obtainCoveragePlan() throws ExecutionException, InterruptedException {

        final RiakClient client = new RiakClient(cluster);
        String query = "select * from " + tableName + " where time >= " + from + " and time <= " + to
                + "  and user = 'user1' and geohash = 'hash1'";
        final CoveragePlan cmd = new CoveragePlan.Builder(tableName, query).build();

        final RiakFuture<CoveragePlanResult, String> response = client.executeAsync(cmd);
        response.await();
        final List<CoverageEntry> coverageEntries = new LinkedList<CoverageEntry>();
        for (CoverageEntry ce : response.get()) {
            coverageEntries.add(ce);
        }
        logger.info("Got {} Coverage Entries", coverageEntries.size());
        assertEquals(numberOfQuanta, coverageEntries.size());
    }

    @Test
    public void queryWithCoveragePlan() throws ExecutionException, InterruptedException, UnknownHostException {

        final RiakClient client = new RiakClient(cluster);
        String queryOneRowOnly = "select * from " + tableName + " where time >= " + (from - 100) + " and time < " + (from + 100)
                + " and user = 'user1' and geohash = 'hash1'";

        final CoveragePlan cmd = new CoveragePlan.Builder(tableName, queryOneRowOnly).build();

        final RiakFuture<CoveragePlanResult, String> response = client.executeAsync(cmd);
        response.await();
        final List<CoverageEntry> coverageEntries = new LinkedList<CoverageEntry>();
        for (CoverageEntry ce : response.get()) {
            coverageEntries.add(ce);
        }
        logger.info("Got {} Coverage Entries", coverageEntries.size());
        assertEquals(1, coverageEntries.size());
        CoverageEntry coverageEntry = coverageEntries.get(0);
        HostAndPort hostAndPort = HostAndPort.fromParts(coverageEntry.getHost(), coverageEntry.getPort());

        final RiakNode node = new RiakNode.Builder().withRemoteHost(hostAndPort).withMinConnections(1).build();
        final RiakCluster cl = RiakCluster.builder(node).build();
        cl.start();
        final RiakClient rc = new RiakClient(cl);

        try {
            final Query queryOperation = new Query.Builder(queryOneRowOnly, coverageEntry.getCoverageContext()).build();
            final RiakFuture<QueryResult, String> readResponse = rc.executeAsync(queryOperation);
            response.await();
            assertNotNull(readResponse);
            assertEquals(1, readResponse.get().getRowsCount());
        } finally {
            rc.shutdown();
        }

    }

    @Test
    public void obtainCoveragePlanForNonExistingData() throws ExecutionException, InterruptedException {

        final RiakClient client = new RiakClient(cluster);
        String query = "select * from " + tableName + " where time > " + (from - 10 * quantum) + " and time < "
                + (from - 5 * quantum) + "  and user = 'user1' and geohash = 'hash1'";
        final CoveragePlan cmd = new CoveragePlan.Builder(tableName, query).build();

        final RiakFuture<CoveragePlanResult, String> response = client.executeAsync(cmd);
        response.await();
        final List<CoverageEntry> lst = new LinkedList<CoverageEntry>();
        for (CoverageEntry ce : response.get()) {
            lst.add(ce);
        }
        logger.info("Got {} Coverage Entries", lst.size());
        assertEquals(6, lst.size());
    }

}
