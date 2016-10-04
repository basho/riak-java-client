package com.basho.riak.client.api.commands.indexes.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.UnresolvedConflictException;
import com.basho.riak.client.api.commands.kv.CoveragePlan;
import com.basho.riak.client.api.commands.kv.FullBucketRead;
import com.basho.riak.client.api.commands.kv.FullBucketRead.Response.Entry;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.CoveragePlanOperation.Response.CoverageEntry;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.client.core.util.HostAndPort;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
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
public class ITestFullBucketRead extends ITestBase
{
    final static int minPartitions = 5;
    final static int NUMBER_OF_TEST_VALUES = 100;
    private final static Logger logger = LoggerFactory.getLogger(ITestFullBucketRead.class);

    private static CoveragePlan.Response coveragePlan;
    private static RiakClient client;

    // TODO: Remove assumption as Riak KV with PEx and Coverage plan will be released
    @BeforeClass
    public static void BeforeClass() throws ExecutionException, InterruptedException
    {
        Assume.assumeTrue(testTimeSeries);
        Assume.assumeTrue(testCoveragePlan);

        bucketName = BinaryValue.create("ITestFullBucketRead" + new Random().nextLong());
        setupData();
    }

    @AfterClass
    public static void AfterClass() throws ExecutionException, InterruptedException
    {
        resetAndEmptyBucket(defaultNamespace());
    }

    private static void setupData() throws ExecutionException, InterruptedException
    {
        String indexName = "creationNo";
        String keyBase = "k";
        String value = "v";

        setupIndexTestData(defaultNamespace(), indexName, keyBase, value);

        setupCoveragePlan();

        // To be sure that settle down across nodes
        Thread.sleep(1000);
    }

    private static void setupCoveragePlan() throws ExecutionException, InterruptedException
    {
        final CoveragePlan cmd = CoveragePlan.Builder.create(defaultNamespace())
                                                     .withMinPartitions(minPartitions)
                                                     .build();

        client = new RiakClient(cluster);

        coveragePlan = client.execute(cmd);

        if (logger.isInfoEnabled())
        {
            final StringBuilder builder = new StringBuilder("\n\tGot the following list of Coverage Entries:");
            for (CoverageEntry ce : coveragePlan)
            {
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
    public void readPlainTextValues() throws ExecutionException, InterruptedException, UnknownHostException
    {
        final FullBucketRead cmd = new FullBucketRead.Builder(defaultNamespace())
            .withReturnBody(false)
            .build();

        final FullBucketRead.Response response = client.execute(cmd);

        final List<FullBucketRead.Response.Entry> entries = response.getEntries();
        assertEquals(NUMBER_OF_TEST_VALUES, entries.size());

        final HashSet<String> returnedKeys = new HashSet<>(NUMBER_OF_TEST_VALUES);
        for (FullBucketRead.Response.Entry e : entries)
        {
            assertFalse(e.hasFetchedValue());
            assertNull(e.getFetchedValue());
            returnedKeys.add(e.getLocation().getKeyAsString());
        }

        assertEquals(NUMBER_OF_TEST_VALUES, returnedKeys.size());
    }

    @Test
    public void readPlainTextValuesWithReturnBody()
            throws ExecutionException, InterruptedException, UnknownHostException
    {
        final FullBucketRead cmd = new FullBucketRead.Builder(defaultNamespace())
                .withReturnBody(true)
                .build();

        final FullBucketRead.Response response = client.execute(cmd);

        final List<FullBucketRead.Response.Entry> entries = response.getEntries();
        assertEquals(NUMBER_OF_TEST_VALUES, entries.size());

        for (FullBucketRead.Response.Entry e : entries)
        {
            assertTrue(e.hasFetchedValue());
            final RiakObject ro = e.getFetchedValue().getValue(RiakObject.class);

            final int expectedValue = Integer.parseInt(e.getLocation().getKeyAsString().substring(1));
            assertEquals("v" + expectedValue, ro.getValue().toString());

            assertEquals("plain/text", ro.getContentType());
        }
    }

    @Test
    public void readPlainTextValuesWithCoverageContext()
            throws ExecutionException, InterruptedException, UnknownHostException
    {
        final Map<String, RiakObject> results = performFBReadWithCoverageContext(false, false);
        assertEquals(NUMBER_OF_TEST_VALUES, results.size());

        for (int i=0; i<NUMBER_OF_TEST_VALUES; ++i)
        {
            final String key = "k"+i;
            assertTrue(results.containsKey(key));

            // since returnBody=false, riakObject mustn't be returned
            assertNull(results.get(key));
        }
    }

    @Test
    public void readPlainTextValuesWithCoverageContextContinuouslyWithReturnBody()
            throws ExecutionException, InterruptedException, UnknownHostException
    {
        final Map<String, RiakObject> results = performFBReadWithCoverageContext(true, true);
        assertEquals(NUMBER_OF_TEST_VALUES, results.size());

        for (int i=0; i<NUMBER_OF_TEST_VALUES; ++i)
        {
            final String key = "k"+i;
            assertTrue(results.containsKey(key));

            final RiakObject ro = results.get(key);
            assertNotNull(ro);

            assertEquals("plain/text", ro.getContentType());
            assertFalse(ro.isDeleted());
            assertEquals("v"+i, ro.getValue().toStringUtf8());
        }
    }

    @Test
    public void queryDataByUsingAnAlternateCoveragePlan()
            throws ExecutionException, InterruptedException, UnknownHostException
    {
        final List<CoverageEntry> coverageEntries = new LinkedList<>();
        for (CoverageEntry ce : coveragePlan)
        {
            coverageEntries.add(ce);
        }
        assertTrue(coverageEntries.size() > minPartitions);

        final CoverageEntry failedEntry = coverageEntries.get(0); // assume this coverage entry failed
        logger.debug("Reading original data by coverage entry: {}", failedEntry);
        final Map<String, RiakObject> riakObjects = readDataForCoverageEntry(failedEntry);
        assertNotNull(riakObjects);

        // build request for alternative coverage plan
        logger.debug("Querying alternative coverage plan for coverage entry: {}", failedEntry);
        final CoveragePlan cmd = CoveragePlan.Builder.create(defaultNamespace())
                .withMinPartitions(minPartitions)
                .withReplaceCoverageEntry(failedEntry)
                .withUnavailableCoverageEntries(Collections.singletonList(failedEntry))
                .build();
        final CoveragePlan.Response response = client.execute(cmd);
        assertTrue(response.iterator().hasNext());

        final CoverageEntry alternativeEntry = response.iterator().next();
        logger.debug("Reading original data by alternative coverage entry: {}", alternativeEntry);
        final Map<String, RiakObject> riakObjectsAlternative = readDataForCoverageEntry(alternativeEntry);
        assertEquals("Key set queried by alternative coverage entry does not match original keys",
                riakObjects.keySet(), riakObjectsAlternative.keySet());
        for (Map.Entry<String,RiakObject> entry : riakObjects.entrySet())
        {
            assertEquals(entry.getValue().getValue(), riakObjectsAlternative.get(entry.getKey()).getValue());
        }
    }

    private Map<String, RiakObject> performFBReadWithCoverageContext(boolean withContinuation,boolean withReturnBody)
            throws UnknownHostException, ExecutionException, InterruptedException
    {
        final Map<CoverageEntry, List<Entry>> chunkedKeys = new HashMap<>();

        // -- perform Full Bucket read and gather all results preserving partitioning by CoverageEntry
        for (HostAndPort host : coveragePlan.hosts())
        {
            final RiakNode node = new RiakNode.Builder()
                .withRemoteHost(host)
                .withMinConnections(1)
                .build();

            final RiakCluster cl = RiakCluster.builder(node)
                .build();

            cl.start();

            final RiakClient rc = new RiakClient(cl);
            try
            {
                for (CoverageEntry ce : coveragePlan.hostEntries(host))
                {
                    final Map<CoverageEntry, List<Entry>> keys =
                            retrieveChunkedKeysForCoverageEntry(rc, ce, withContinuation, withReturnBody);
                    chunkedKeys.putAll(keys);
                }
            }
            finally
            {
                rc.shutdown();
            }
        }

        // -- Transform results
        return transformChunkedKeysToRiakObjects(chunkedKeys);
    }

    private Map<String, RiakObject> readDataForCoverageEntry(CoverageEntry ce)
            throws UnknownHostException, ExecutionException, InterruptedException
    {
        final RiakNode node = new RiakNode.Builder()
            .withRemoteHost(HostAndPort.fromParts(ce.getHost(),ce.getPort()))
            .withMinConnections(1)
            .build();

        final RiakCluster cl = RiakCluster.builder(node)
            .build();

        cl.start();

        final RiakClient rc = new RiakClient(cl);

        Map<CoverageEntry, List<Entry>> keys = Collections.emptyMap();
        try
        {
            keys = retrieveChunkedKeysForCoverageEntry(rc, ce, true, true);
        }
        finally
        {
            rc.shutdown();
        }

        return transformChunkedKeysToRiakObjects(keys);
    }

    @SuppressWarnings("unchecked")
    private Map<CoverageEntry, List<Entry>> retrieveChunkedKeysForCoverageEntry(RiakClient rc,
                                                                                CoverageEntry ce,
                                                                                boolean withContinuation,
                                                                                boolean withReturnBody)
            throws ExecutionException, InterruptedException
    {
        final Map<CoverageEntry, List<Entry>> chunkedKeys = new HashMap<>();
        if (!withContinuation)
        {
            final FullBucketRead cmd2 = new FullBucketRead.Builder(defaultNamespace(), ce.getCoverageContext())
                .withReturnBody(withReturnBody)
                .build();

            final FullBucketRead.Response readResponse = rc.execute(cmd2);
            chunkedKeys.put(ce, readResponse.getEntries());
        }
        else
        {
            BinaryValue continuation = null;
            final List<Entry> data = new LinkedList<>();

            do
            {
                final FullBucketRead cmd2 = new FullBucketRead.Builder(defaultNamespace(), ce.getCoverageContext())
                    .withReturnBody(withReturnBody)
                    .withMaxResults(2)
                    .withPaginationSort(true)
                    .withContinuation(continuation)
                    .build();

                final FullBucketRead.Response r = rc.execute(cmd2);
                final List<Entry> entries;

                if (r.hasEntries())
                {
                    entries = r.getEntries();
                    data.addAll(entries);
                }
                else
                {
                    entries = Collections.EMPTY_LIST;
                }

                logger.debug("FullBucketRead query(ce={}, token={}) returns:\n  token: {}\n  entries: {}",
                    continuation, ce, r.getContinuation(), entries);

                continuation = r.getContinuation();
            }
            while (continuation != null);
            chunkedKeys.put(ce, data);
        }

        return chunkedKeys;
    }

    private Map<String, RiakObject> transformChunkedKeysToRiakObjects(Map<CoverageEntry, List<Entry>> chunkedKeys)
            throws UnresolvedConflictException
    {
        final Map<String, RiakObject> results = new HashMap<>(chunkedKeys.size());
        for (Map.Entry<CoverageEntry, List<Entry>> e : chunkedKeys.entrySet())
        {
            final CoverageEntry ce = e.getKey();
            if (e.getValue().isEmpty())
            {
                logger.warn("Nothing was returned for {}", ce);
            }
            else
            {
                for (FullBucketRead.Response.Entry re : e.getValue())
                {
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
}
