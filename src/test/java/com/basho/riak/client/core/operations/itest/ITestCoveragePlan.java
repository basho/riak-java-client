package com.basho.riak.client.core.operations.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.indexes.BinIndexQuery;
import com.basho.riak.client.api.commands.kv.CoveragePlan;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.CoveragePlanOperation.Response.CoverageEntry;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.indexes.LongIntIndex;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.client.core.util.HostAndPort;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Sergey Galkin <sgalkin at basho dot com>
 */
public class ITestCoveragePlan extends ITestBase {
    private final static Logger logger = LoggerFactory.getLogger(ITestCoveragePlan.class);

    @Test
    public void obtainCoveragePlan() throws ExecutionException, InterruptedException {
        final int minPartitions = 5;
        final RiakClient client = new RiakClient(cluster);
        final CoveragePlan cmd = CoveragePlan.Builder.create(defaultNamespace())
                .withMinPartitions(minPartitions)
                .build();

        final CoveragePlan.Response response = client.execute(cmd);
        final List<CoverageEntry> lst = new LinkedList<CoverageEntry>();
        for(CoverageEntry ce: response){
            lst.add(ce);
        }
        logger.info("Got the following list of Coverage Entries: {}", lst);

        Assert.assertTrue(lst.size() > minPartitions);
    }

    @Test
    public void fetchAllDataByUsingCoverageContext() throws ExecutionException, InterruptedException, UnknownHostException {
        String indexName = "test_coverctx_index";
        String keyBase = "k";
        String value = "v";

        setupIndexTestData(defaultNamespace(), indexName, keyBase, value);

        final RiakClient client = new RiakClient(cluster);
        final CoveragePlan cmd = CoveragePlan.Builder.create(defaultNamespace())
                .build();

        final CoveragePlan.Response response = client.execute(cmd);

        if(logger.isInfoEnabled()) {
            StringBuilder sbld = new StringBuilder("Got the following list of Coverage Entries:");
            for (CoverageEntry ce: response) {
                sbld.append(String.format("\n\t%s:%d ('%s')",
                        ce.getHost(),
                        ce.getPort(),
                        ce.getDescription()
                ));
            }
            logger.info(sbld.toString());
        }

        final Map<CoverageEntry, List<BinIndexQuery.Response.Entry>> chunkedKeys
                = new HashMap<CoverageEntry, List<BinIndexQuery.Response.Entry>>();

        for(HostAndPort host: response.hosts()) {
            final RiakNode node= new RiakNode.Builder()
                    .withRemoteHost(host)
                    .withMinConnections(1)
                    .build();

            final RiakCluster cl = RiakCluster.builder(node)
                    .build();

            cl.start();

            final RiakClient rc = new RiakClient(cl);
            try {
                for(CoverageEntry ce: response.hostEntries(host)) {
                    // The only "$bucket" Binary Index may be used for Full Bucket Reads
                    final BinIndexQuery query = new BinIndexQuery.Builder(defaultNamespace(), "$bucket", ce.getCoverageContext())
                            .withCoverageContext(ce.getCoverageContext())
                            .withTimeout(2000)
                            .build();

                    final BinIndexQuery.Response readResponse = rc.execute(query);

                    chunkedKeys.put(ce, readResponse.getEntries());
                    assertNotNull(readResponse);
                }
            } finally {
                rc.shutdown();
            }
        }

        final Set<String> keys = new HashSet<String>(100);
        for(Map.Entry<CoverageEntry, List<BinIndexQuery.Response.Entry>> e: chunkedKeys.entrySet()){
            final CoverageEntry ce = e.getKey();
            if(e.getValue().isEmpty()){
                logger.debug("Nothing was returned for CE {}", ce);
            } else {
                final List<String> lst = new ArrayList<String>(e.getValue().size());

                for(BinIndexQuery.Response.Entry re: e.getValue()){
                    lst.add(re.getRiakObjectLocation().getKeyAsString());
                }

                logger.debug("{} keys were returned for CE {}:\n\t{}",
                        e.getValue().size(), ce, lst
                    );

                keys.addAll(lst);
            }
        }

        assertEquals(100, keys.size());
    }

    /**
     * Copy & pasted from ITestSecondaryIndexQueryOp
     */
    private void setupIndexTestData(Namespace ns, String indexName, String keyBase, String value)
            throws InterruptedException, ExecutionException
    {
        for (long i = 0; i < 100; i++)
        {
            RiakObject obj = new RiakObject().setValue(BinaryValue.create(value));

            obj.getIndexes().getIndex(LongIntIndex.named(indexName)).add(i);

            Location location = new Location(ns, keyBase + i);
            StoreOperation storeOp =
                    new StoreOperation.Builder(location)
                            .withPw(Quorum.allQuorum().getIntValue())
                            .withContent(obj)
                            .withTimeout(2000)
                            .build();

            cluster.execute(storeOp);
            storeOp.get();
        }
    }
}
