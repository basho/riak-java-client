package com.basho.riak.client.core.operations.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.buckets.FetchBucketProperties;
import com.basho.riak.client.api.commands.indexes.BinIndexQuery;
import com.basho.riak.client.api.commands.kv.CoveragePlan;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.CoveragePlanOperation.Response.CoverageEntry;
import com.basho.riak.client.core.operations.FetchBucketPropsOperation;
import com.basho.riak.client.core.query.BucketProperties;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.HostAndPort;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.*;

/**
 * @author Sergey Galkin <sgalkin at basho dot com>
 */
public class ITestCoveragePlan extends ITestBase {
    private final static Logger logger = LoggerFactory.getLogger(ITestCoveragePlan.class);

    @Rule
    public ExpectedException exception = ExpectedException.none();


    @Test
    public void obtainAlternativeCoveragePlan() throws ExecutionException, InterruptedException {
        final int minPartitions = 5;
        final RiakClient client = new RiakClient(cluster);
        CoveragePlan cmd = CoveragePlan.Builder.create(defaultNamespace())
                .withMinPartitions(minPartitions)
                .build();

        CoveragePlan.Response response = client.execute(cmd);
        final List<CoverageEntry> coverageEntries = new ArrayList<>();
        for (CoverageEntry ce : response) {
            coverageEntries.add(ce);
        }
        Assert.assertTrue(coverageEntries.size() > minPartitions);

        final CoverageEntry failedEntry = coverageEntries.get(0); // assume this coverage entry failed

        // build request for alternative coverage plan
        cmd = CoveragePlan.Builder.create(defaultNamespace())
                .withMinPartitions(minPartitions)
                .withReplaceCoverageEntry(failedEntry)
                .withUnavailableCoverageEntries(Collections.singletonList(failedEntry))
                .build();
        response = client.execute(cmd);
        Assert.assertTrue(response.iterator().hasNext());

        final HostAndPort failedHostAndPort = HostAndPort.fromParts(failedEntry.getHost(), failedEntry.getPort());
        for (CoverageEntry ce : response) {
            HostAndPort alternativeHostAndPort = HostAndPort.fromParts(ce.getHost(), ce.getPort());

            // received coverage entry must not be on the same host
            assertThat(alternativeHostAndPort, not(equalTo(failedHostAndPort)));
            assertThat(ce.getCoverageContext(), not(equalTo(failedEntry.getCoverageContext())));
        }
    }

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

        final Set<String> keys = new HashSet<String>(NUMBER_OF_TEST_VALUES);
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

        assertEquals(NUMBER_OF_TEST_VALUES, keys.size());
    }

    @Test
    public void alternativeCoveragePlanShouldFailIfNoPrimaryPartitionAvailable() throws ExecutionException, InterruptedException, UnknownHostException {
        final RiakClient client = new RiakClient(cluster);

        final Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());
        final FetchBucketProperties fetchProps = new FetchBucketProperties.Builder(ns).build();
        final FetchBucketPropsOperation.Response fetchResponse = client.execute(fetchProps);
        final BucketProperties bp = fetchResponse.getBucketProperties();
        final Integer nVal = bp.getNVal();

        final int minPartitions = 5;

        final CoveragePlan cmd = CoveragePlan.Builder.create(defaultNamespace())
            .withMinPartitions(minPartitions)
            .build();

        final CoveragePlan.Response response = client.execute(cmd);
        final List<CoverageEntry> coverageEntries = new LinkedList<>();
        for (CoverageEntry ce : response) {
            coverageEntries.add(ce);
        }

        CoverageEntry failedEntry = coverageEntries.get(0);
        List<CoverageEntry> unavailableCoverageEntries = new LinkedList<>();

        for (int i = 0; i < nVal-1; i++) {
            unavailableCoverageEntries.add(failedEntry);

            final CoveragePlan cmdAlternative = CoveragePlan.Builder.create(defaultNamespace())
                .withMinPartitions(minPartitions)
                .withReplaceCoverageEntry(failedEntry)
                .withUnavailableCoverageEntries(unavailableCoverageEntries)
                .build();
            final CoveragePlan.Response responseAlternative = client.execute(cmdAlternative);
            Assert.assertTrue(responseAlternative.iterator().hasNext());
            failedEntry = responseAlternative.iterator().next();
        }

        unavailableCoverageEntries.add(failedEntry);

        //At this moment all virtual nodes for coverage entry are down
        //request to get alternative coverage entry should fail
        final CoveragePlan cmdAlternativeFailing = CoveragePlan.Builder.create(defaultNamespace())
            .withMinPartitions(minPartitions)
            .withReplaceCoverageEntry(failedEntry)
            .withUnavailableCoverageEntries(unavailableCoverageEntries)
            .build();

        exception.expect(ExecutionException.class);
        exception.expectMessage("com.basho.riak.client.core.netty.RiakResponseException: primary_partition_unavailable");

        client.execute(cmdAlternativeFailing);
    }
}
