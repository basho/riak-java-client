package com.basho.riak.client.api;


import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

public class ITestClusterLifecycle
{
    protected static RiakCluster cluster;
    protected static boolean testLifecycle;
    protected static String hostname;
    protected static int pbcPort;
    protected static Random random = new Random();

    public ITestClusterLifecycle()
    {
        testLifecycle = Boolean.getBoolean("com.basho.riak.lifecycle");
        hostname = System.getProperty("com.basho.riak.host", RiakNode.Builder.DEFAULT_REMOTE_ADDRESS);

        pbcPort = Integer.getInteger("com.basho.riak.pbcport", RiakNode.Builder.DEFAULT_REMOTE_PORT);

        RiakNode.Builder builder = new RiakNode.Builder()
                                               .withRemoteAddress(hostname)
                                               .withRemotePort(10017)
                                               .withMinConnections(1)
                                               .withMaxConnections(1);

        cluster = new RiakCluster.Builder(builder.build()).build();
    }

    @Test
    public void testManyCommandsOneConnection() throws InterruptedException, ExecutionException, TimeoutException
    {
        RiakClient client = null;
        try
        {
            client = new RiakClient(cluster);
            cluster.start();

            final Namespace namespace = new Namespace("plain", Integer.toString(random.nextInt()));

            for (int i = 0; i < 10000; i++)
            {
                createAndStoreObject(client, new Location(namespace, Integer.toString(i)));
            }

        }
        catch (Exception ex)
        {
            assertNull(ex);
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
        final StoreValue.Response response = execute.get(1, TimeUnit.SECONDS);

        assertNotNull(response);
    }
}
