package com.basho.riak.client.core.operations.itest;

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import org.junit.After;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertNotNull;

public class ITestPlaintextAuthConnection
{
    private static boolean security;
    private static boolean tls;
    private RiakCluster cluster;

    @BeforeClass
    public static void setUp()
    {
        /**
         * Riak security.
         *
         * If you want to test AUTH, you need to set up your system as described
         * in the README.md's "Security Tests" section.
         */
        security = Boolean.parseBoolean(System.getProperty("com.basho.riak.security"));
        tls = Boolean.parseBoolean(System.getProperty("com.basho.riak.security.tls"));
    }

    @After
    public void teardown() throws InterruptedException, ExecutionException, TimeoutException
    {
        if (cluster != null)
        {
            cluster.shutdown().get(1, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testPlaintextAuthentication() throws Exception
    {
        Assume.assumeTrue(security);
        Assume.assumeFalse(tls);

        cluster = RiakPlaintextAuthConnection.getRiakCluster("riakpass", "Test1234");

        assertCanGetConnection();
    }

    @Test
    public void testTrustBasedAuthentication() throws Exception
    {
        Assume.assumeTrue(security);
        Assume.assumeFalse(tls);

        cluster = RiakPlaintextAuthConnection.getRiakCluster("riak_trust_user", "riak_trust_user");

        assertCanGetConnection();
    }

    private void assertCanGetConnection() throws Exception
    {
        for (RiakNode node : cluster.getNodes())
        {
            assertNotNull(Whitebox.invokeMethod(node, "getConnection"));
        }
    }
}
