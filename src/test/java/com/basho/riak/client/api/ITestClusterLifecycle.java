package com.basho.riak.client.api;


import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.test.rule.DockerRiakClusterRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

public class ITestClusterLifecycle
{
    protected static RiakCluster cluster;
    protected static boolean testLifecycle;
    protected static String hostname;
    protected static int pbcPort;
    protected static Random random = new Random();
    private final Logger logger = LoggerFactory.getLogger(ITestClusterLifecycle.class);

    @Rule
    public ExpectedException thrown= ExpectedException.none();

    @ClassRule
    public static DockerRiakClusterRule dockerCluster = ITestBase.dockerCluster;

    public ITestClusterLifecycle()
    {
        testLifecycle = Boolean.getBoolean("com.basho.riak.lifecycle");
        hostname = System.getProperty("com.basho.riak.host",
                dockerCluster.getIps().iterator().hasNext() // if cluster was not started default host should be used
                        ? dockerCluster.getIps().iterator().next()
                        : RiakNode.Builder.DEFAULT_REMOTE_ADDRESS);

        pbcPort = Integer.getInteger("com.basho.riak.pbcport", RiakNode.Builder.DEFAULT_REMOTE_PORT);

        RiakNode.Builder builder = new RiakNode.Builder()
                                               .withRemoteAddress(hostname)
                                               .withRemotePort(pbcPort)
                                               .withMinConnections(1)
                                               .withMaxConnections(1);

        cluster = new RiakCluster.Builder(builder.build()).build();
    }

    @Test
    public void testManyCommandsOneConnection() throws InterruptedException, ExecutionException, TimeoutException
    {
        assumeTrue(testLifecycle);

        RiakClient client = null;
        int i = 0;
        try
        {
            client = new RiakClient(cluster);
            cluster.start();

            final Namespace namespace = new Namespace("plain", Integer.toString(random.nextInt()));

            for (i=0; i < 1000; i++)
            {
                createAndStoreObject(client, new Location(namespace, Integer.toString(i)));
            }

        }
        catch (Exception ex)
        {
            logger.debug("Exception occurred", ex);
            logger.debug("Cluster state: {}, iteration: {}", cluster.getNodes().get(0).getNodeState().toString(), i);
            fail(ex.getMessage());
        }
        finally
        {
            final Boolean shutdownClean = client.shutdown().get(3, TimeUnit.SECONDS);
            assertTrue(shutdownClean);
        }
    }

    @Test
    public void testThatConnectionsGetReturnedBeforeErrors() throws InterruptedException, ExecutionException, TimeoutException
    {
        assumeTrue(testLifecycle);

        thrown.expect(ExecutionException.class);
        thrown.expectMessage(containsString("no_type"));

        RiakClient client = null;

        try
        {
            client = new RiakClient(cluster);
            cluster.start();

            final Namespace namespace = new Namespace("doesnotexist", Integer.toString(random.nextInt()));
            createAndStoreObject(client, new Location(namespace, "no_type"));
        }
        finally
        {
            final Boolean shutdownClean = client.shutdown().get(3, TimeUnit.SECONDS);
            assertTrue(shutdownClean);
        }
    }

    private void createAndStoreObject(RiakClient client, Location location)
            throws ExecutionException, InterruptedException, TimeoutException
    {
        final StoreValue storeCmd = new StoreValue.Builder("value").withLocation(location).build();
        final RiakFuture<StoreValue.Response, Location> execute = client.executeAsync(storeCmd);
        final StoreValue.Response response = execute.get();

        assertNotNull(response);
    }
}
