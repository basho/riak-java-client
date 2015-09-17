package com.basho.riak.client.api.commands.indexes.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.FullBucketRead;
import com.basho.riak.client.api.commands.kv.FullBucketRead.Response.Entry;
import com.basho.riak.client.api.commands.kv.CoveragePlan;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.CoveragePlanOperation;
import com.basho.riak.client.core.operations.CoveragePlanOperation.Response.CoverageEntry;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.client.core.util.HostAndPort;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * @author Sergey Galkin <sgalkin at basho dot com>
 */
public class ITestFullBucketRead extends ITestBase {
    final static int NUMBER_OF_TEST_VALUES = 100;
    private final static Logger logger = LoggerFactory.getLogger(ITestFullBucketRead.class);

    private CoveragePlan.Response coveragePlan;
    private RiakClient client;

    @Before
    public void setupData() throws ExecutionException, InterruptedException {
        String indexName = "creationNo";
        String keyBase = "k";
        String value = "v";

        setupIndexTestData(defaultNamespace(), indexName, keyBase, value);

        final CoveragePlan cmd = CoveragePlan.Builder.create(defaultNamespace())
                .build();

        client = new RiakClient(cluster);

        coveragePlan = client.execute(cmd);

        if (logger.isInfoEnabled()) {
            final StringBuilder builder = new StringBuilder("\n\tGot the following list of Coverage Entries:");
            for (CoveragePlanOperation.Response.CoverageEntry ce : coveragePlan) {
                builder.append(String.format("\n\t%s:%d ('%s')",
                        ce.getHost(),
                        ce.getPort(),
                        ce.getDescription()
                ));
            }
            logger.info(builder.toString());
        }
    }

    @Test
    public void readPlainTextValues() throws ExecutionException, InterruptedException, UnknownHostException {

        final FullBucketRead cmd = new FullBucketRead.Builder(defaultNamespace())
                .withReturnBody(false)
                .build();

        final FullBucketRead.Response response = client.execute(cmd);

        final List<FullBucketRead.Response.Entry> entries = response.getEntries();
        assertEquals(NUMBER_OF_TEST_VALUES, entries.size());

        final HashSet<String> returnedKeys = new HashSet<String>(NUMBER_OF_TEST_VALUES);
        for (FullBucketRead.Response.Entry e : entries) {
            assertFalse(e.hasFetchedValue());
            assertNull(e.getFetchedValue());
            returnedKeys.add(e.getLocation().getKeyAsString());
        }

        assertEquals(NUMBER_OF_TEST_VALUES, returnedKeys.size());
    }

    @Test
    public void readPlainTextValuesWithReturnBody() throws ExecutionException, InterruptedException, UnknownHostException {

        final FullBucketRead cmd = new FullBucketRead.Builder(defaultNamespace())
                .withReturnBody(true)
                .build();

        final FullBucketRead.Response response = client.execute(cmd);

        final List<FullBucketRead.Response.Entry> entries = response.getEntries();
        assertEquals(NUMBER_OF_TEST_VALUES, entries.size());

        for (FullBucketRead.Response.Entry e : entries) {
            assertTrue(e.hasFetchedValue());
            final RiakObject ro = e.getFetchedValue().getValue(RiakObject.class);

            final int expectedValue = Integer.parseInt(e.getLocation().getKeyAsString().substring(1));
            assertEquals("v" + expectedValue, ro.getValue().toString());

            assertEquals("plain/text", ro.getContentType());
        }
    }

    @Test
    public void readPlainTextValuesWithCoverageContext() throws ExecutionException, InterruptedException, UnknownHostException {
        final Map<String, RiakObject> results = performFBReadWithCoverageContext(false, false);
        assertEquals(NUMBER_OF_TEST_VALUES, results.size());

        for(int i=0; i<NUMBER_OF_TEST_VALUES; ++i){
            final String key = "k"+i;
            assertTrue(results.containsKey(key));

            // since returnBody=false, riakObject mustn't be returned
            assertNull(results.get(key));
        }
    }

    @Test
    public void readPlainTextValuesWithCoverageContextContinuouslyWithReturnBody() throws ExecutionException, InterruptedException, UnknownHostException {
        final Map<String, RiakObject> results = performFBReadWithCoverageContext(true, true);
        assertEquals(NUMBER_OF_TEST_VALUES, results.size());

        for(int i=0; i<NUMBER_OF_TEST_VALUES; ++i){
            final String key = "k"+i;
            assertTrue(results.containsKey(key));

            final RiakObject ro = results.get(key);
            assertNotNull(ro);

            assertEquals("plain/text", ro.getContentType());
            assertFalse(ro.isDeleted());
            assertEquals("v"+i, ro.getValue().toStringUtf8());
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, RiakObject> performFBReadWithCoverageContext(boolean withContinuation,
               boolean withReturnBody) throws UnknownHostException, ExecutionException, InterruptedException {

        final Map<CoverageEntry, List<Entry>> chunkedKeys = new HashMap<CoveragePlanOperation.Response.CoverageEntry, List<FullBucketRead.Response.Entry>>();

        // -- perform Full Bucket read and gather all results preserving partitioning by CoverageEntry
        for (HostAndPort host : coveragePlan.hosts()) {
            final RiakNode node = new RiakNode.Builder()
                    .withRemoteHost(host)
                    .withMinConnections(1)
                    .build();

            final RiakCluster cl = RiakCluster.builder(node)
                    .build();

            cl.start();

            final RiakClient rc = new RiakClient(cl);
            try {
                for (CoverageEntry ce : coveragePlan.hostEntries(host)) {

                    if (!withContinuation) {
                        final FullBucketRead cmd2 = new FullBucketRead.Builder(defaultNamespace(), ce.getCoverageContext())
                                .withReturnBody(withReturnBody)
                                .build();

                        final FullBucketRead.Response readResponse = rc.execute(cmd2);
                        chunkedKeys.put(ce, readResponse.getEntries());
                    } else {
                        BinaryValue continuation = null;
                        final List<Entry> data = new LinkedList<FullBucketRead.Response.Entry>();

                        do {
                            final FullBucketRead cmd2 = new FullBucketRead.Builder(defaultNamespace(), ce.getCoverageContext())
                                    .withReturnBody(withReturnBody)
                                    .withMaxResults(2)
                                    .withPaginationSort(true)
                                    .withContinuation(continuation)
                                    .build();

                            final FullBucketRead.Response r = rc.execute(cmd2);
                            final List<Entry> entries;

                            if (r.hasEntries()) {
                                entries = r.getEntries();
                                data.addAll(entries);
                            } else {
                                entries = Collections.EMPTY_LIST;
                            }

                            logger.debug("FullBucketRead query(ce={}, token={}) returns:\n  token: {}\n  entries: {}",
                                    continuation, ce, r.getContinuation(), entries);

                            continuation = r.getContinuation();
                        } while (continuation != null);
                        chunkedKeys.put(ce, data);
                    }
                }
            } finally {
                rc.shutdown();
            }
        }

        // -- Transform results
        final HashMap<String, RiakObject> results = new HashMap<String, RiakObject>(100);
        for (Map.Entry<CoverageEntry, List<Entry>> e : chunkedKeys.entrySet()) {
            final CoverageEntry ce = e.getKey();
            if (e.getValue().isEmpty()) {
                logger.warn("Nothing was returned for {}", ce);
            } else {
                for (FullBucketRead.Response.Entry re : e.getValue()) {
                    final RiakObject ro = re.hasFetchedValue() ? re.getFetchedValue().getValue(RiakObject.class) : null;
                    results.put(re.getLocation().getKeyAsString(), ro);
                }

                logger.debug("{} keys were returned for {}:\n\t{}",
                        e.getValue().size(), ce, e.getValue()
                );
            }
        }
        return results;
    }

    // TODO: Add test for JSON values
}
