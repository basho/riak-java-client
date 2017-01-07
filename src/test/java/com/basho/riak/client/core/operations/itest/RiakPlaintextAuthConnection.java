package com.basho.riak.client.core.operations.itest;

import java.io.IOException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;

public class RiakPlaintextAuthConnection
{
    /**
     * Riak PBC port
     *
     * In case you want/need to use a custom PBC port you may pass it by using the following system property
     */
    private static final int testRiakPort = Integer.getInteger("com.basho.riak.pbcport", RiakNode.Builder.DEFAULT_REMOTE_PORT);

    /**
     * Initialize Cluster
     * @param builder all builder properties required to make connection to a node.
     * @return Riak Cluster object based on builder properties
     */
    private static RiakCluster initializeRiakCluster(RiakNode.Builder builder)
    {
        RiakCluster cluster = new RiakCluster.Builder(builder.build()).build();
        cluster.start();
        return cluster;
    }

    /**
     * Get Riak Cluster Handle. This is for unsecured connection when Riak security is disabled.
     * @return Riak Cluster Object
     */
    public static RiakCluster getRiakCluster()
    {
        RiakNode.Builder builder = createRiakNodeBuilder();
        RiakCluster cluster = initializeRiakCluster(builder);

        return cluster;
    }

    /**
     * Get Riak Client Handle. This is for unsecured connection when Riak security is disabled.
     * @return Riak Client Object
     */
    public static RiakClient getRiakConnection()
    {
        RiakClient client = null;
        RiakCluster cluster = getRiakCluster();
        if (cluster != null)
        {
            client = new RiakClient(cluster);
        }

        return client;
    }

    /**
     * Get Riak Cluster Handle. This is for authenticated connection, no encryption. Riak security is enabled.
     * @param username username with which the connection needs to be established
     * @param password password of the username provided
     * @return Riak Cluster Object
     */
    public static RiakCluster getRiakCluster(String username, String password)
    {
        RiakNode.Builder builder = createRiakNodeBuilder();
        builder.withAuth(username, password);
        builder.withTls(false, false);
        RiakCluster cluster = initializeRiakCluster(builder);

        return cluster;
    }

    /**
     * Get Riak Client Handle. This is for authenticated connection, but not encrypted. Riak security is enabled.
     * @param username username with which the connection needs to be established
     * @param password password of the username provided
     * @return Riak Client Object
     */
    public static RiakClient getRiakConnection(String username, String password)
    {
        RiakClient client = null;
        RiakCluster cluster = getRiakCluster(username,password);
        if (cluster != null)
        {
            client = new RiakClient(cluster);
        }

        return client;
    }

    private static RiakNode.Builder createRiakNodeBuilder()
    {
        RiakNode.Builder builder = new RiakNode.Builder();
        builder.withMinConnections(1);
        builder.withRemotePort(testRiakPort);
        return builder;
    }
}
