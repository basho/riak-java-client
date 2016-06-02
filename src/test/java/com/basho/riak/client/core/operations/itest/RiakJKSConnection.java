package com.basho.riak.client.core.operations.itest;

import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;

public class RiakJKSConnection
{

    private static final String trustStorePasswd = "riak123";
    private static final String trustStrorePath = "truststore.jks";
    private static final KeyStore trustStore = loadKeystore(trustStrorePath,trustStorePasswd);

    /**
     * Riak PBC port
     *
     * In case you want/need to use a custom PBC port you may pass it by using the following system property
     */
    private static final int testRiakPort = Integer.getInteger("com.basho.riak.pbcport", RiakNode.Builder.DEFAULT_REMOTE_PORT);

    /**
     * Load Keystore using the storeFilePath and storePassword
     * @param storePath path to the Keystore file
     * @param storePasswd passowrd of Keystore file
     * @return a keystore with all certificate entries loaded
     */
    private static KeyStore loadKeystore(String storePath, String storePasswd)
    {
        KeyStore store = null;
        try
        {
            store = KeyStore.getInstance("JKS");
            store.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(storePath), storePasswd.toCharArray());
        }
        catch (KeyStoreException | NoSuchAlgorithmException | IOException | CertificateException e)
        {
            e.printStackTrace();
        }
        return store;
    }

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
        if (cluster!=null)
        {
            client= new RiakClient(cluster);
        }
        return client;
    }

    /**
     * Get Riak Cluster Handle. This is for secured connection with user source set to either Trust or Password. Riak security is enabled.
     * @param username username with which the connection needs to be established
     * @param password password of the username provided
     * @return Riak Cluster Object
     */
    public static RiakCluster getRiakCluster(String username, String password)
    {
        RiakNode.Builder builder = createRiakNodeBuilder();
        builder.withAuth(username, password, trustStore);
        RiakCluster cluster = initializeRiakCluster(builder);

        return cluster;
    }

    /**
     * Get Riak Client Handle. This is for secured connection with user source set to either Trust or Password. Riak security is enabled.
     * @param username username with which the connection needs to be established
     * @param password password of the username provided
     * @return Riak Client Object
     */
    public static RiakClient getRiakConnection(String username, String password)
    {
        RiakClient client = null;
        RiakCluster cluster = getRiakCluster(username,password);
        if (cluster!=null)
        {
            client= new RiakClient(cluster);
        }
        return client;
    }

    /**
     * Get Riak Cluster Handle. This is for secured connection with user source set to certificate. Riak security is enabled.
     * @param username username with which the connection needs to be established
     * @param password password of the username provided
     * @param storePath path to the Keystore file containing private and public key of the user.
     * @param storePasswd passowrd of Keystore file.
     * @param keyPasswd password of the user's private key/certificate.
     * @return Riak Cluster Object
     */
    public static RiakCluster getRiakCluster(String username, String password, String storePath, String storePasswd, String keyPasswd)
    {
        KeyStore keyStore = loadKeystore(storePath,storePasswd);

        RiakNode.Builder builder = createRiakNodeBuilder();
        builder.withAuth(username, password, trustStore, keyStore, keyPasswd);
        RiakCluster cluster = initializeRiakCluster(builder);

        return cluster;
    }

    /**
     * Get Riak Client Handle. This is for secured connection with user source set to certificate. Riak security is enabled.
     * @param username username with which the connection needs to be established
     * @param password password of the username provided
     * @param storePath path to the Keystore file containing private and public key of the user.
     * @param storePasswd passowrd of Keystore file.
     * @param keyPasswd password of the user's private key/certificate.
     * @return Riak Client Object
     */
    public static RiakClient getRiakConnection(String username, String password, String storePath, String storePasswd, String keyPasswd)
    {
        RiakClient client = null;
        RiakCluster cluster = getRiakCluster(username,password,storePath,storePasswd,keyPasswd);
        if (cluster!=null)
        {
            client= new RiakClient(cluster);
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
