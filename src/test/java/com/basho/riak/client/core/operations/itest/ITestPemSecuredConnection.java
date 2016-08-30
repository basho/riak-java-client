package com.basho.riak.client.core.operations.itest;

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import org.junit.After;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import static org.junit.Assert.assertNotNull;

public class ITestPemSecuredConnection
{
    private static boolean security;
    private static boolean securityClientCert;
    private RiakCluster cluster;

    @BeforeClass
    public static void setUp()
    {
        /**
         * Riak security.
         *
         * If you want to test SSL/AUTH, you need to set up your system as described
         * in the README.md's "Security Tests" section.
         */

        security = Boolean.parseBoolean(System.getProperty("com.basho.riak.security"));
        securityClientCert = Boolean.parseBoolean(System.getProperty("com.basho.riak.security.clientcert"));
    }

    @After
    public void teardown()
    {
        if (cluster != null)
        {
            cluster.shutdown();
        }
    }

    @Test
    public void testCetificateBasedAuthentication() throws Exception
    {
        Assume.assumeTrue(securityClientCert);
        cluster = RiakPemConnection.getRiakCluster("riakuser",
                                                   "riakuser",
                                                   "riakuser-client-cert-key_pkcs8.pem",
                                                   "riakuser-client-cert.pem");

        assertCanGetConnection();
    }

    @Test
    public void testTrustBasedAuthentication() throws Exception
    {
        Assume.assumeTrue(security);
        cluster = RiakPemConnection.getRiakCluster("riak_trust_user", "riak_trust_user");

        assertCanGetConnection();
    }

    @Test
    public void testPasswordBasedAuthentication() throws Exception
    {
        Assume.assumeTrue(security);
        cluster = RiakPemConnection.getRiakCluster("riakpass", "Test1234");

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
