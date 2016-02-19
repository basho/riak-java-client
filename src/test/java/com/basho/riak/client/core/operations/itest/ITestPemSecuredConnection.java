package com.basho.riak.client.core.operations.itest;

import static org.junit.Assert.assertNotNull;

import org.junit.BeforeClass;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import org.junit.Assume;

public class ITestPemSecuredConnection {

    private static boolean security;
    private static boolean securityClientCert;

    @BeforeClass
    public static void setUp()
    {
        /**
         * Riak security.
         *
         * If you want to test SSL/AUTH, you need to:
         *  1) Setup the certs by running the buildbot makefile's "configure-security-certs" target
         *      cd buildbot;
         *      make configure-security-certs;
         *      cd ../;
         *
         *  2) Copy the certs to your Riak's etc dir, and configure the riak.conf file to use them
         *      resources_dir=./src/test/resources
         *      riak_etc_dir=/fill/in/this/path/
         *
         *      # Shell
         *      cp $resources_dir/cacert.pem $riak_etc_dir
         *      cp $resources_dir/riak-test-cert.pem $riak_etc_dir
         *      cp $resources_dir/riakuser-client-cert.pem $riak_etc_dir
         *
         *      # riak.conf file additions
         *      ssl.certfile = (riak_etc_dir)/cert.pem
         *      ssl.keyfile = (riak_etc_dir)/key.pem
         *      ssl.cacertfile = (riak_etc_dir)/cacert.pem
         *
         *  3) Enable Riak Security
         *      riak-admin security enable
         *
         *  4) create a user "riakuser" with the password "riak_cert_user" and configure it with certificate as a source
         *      riak-admin security add-user riakuser
         *      riak-admin security add-source riakuser 0.0.0.0/0 certificate
         *
         *  5) create a user "riak_trust_user" with the password "riak_trust_user" and configure it with trust as a source
         *      riak-admin security add-user riak_trust_user password=riak_trust_user
         *      riak-admin security add-source riak_trust_user 0.0.0.0/0 trust
         *
         *  6) create a user "riakpass" with the password "riak_passwd_user" and configure it with password as a source
         *      riak-admin security add-user riakpass password=Test1234
         *      riak-admin security add-source riakpass 0.0.0.0/0 password
         *
         *  7) Run the Test suit with the com.basho.riak.security and com.basho.riak.security.clientcert flags set to true
         */

        security = Boolean.parseBoolean(System.getProperty("com.basho.riak.security"));
        securityClientCert = Boolean.parseBoolean(System.getProperty("com.basho.riak.security.clientcert"));
    }

    @Test
    public void testCetificateBasedAuthentication() throws Exception {
        Assume.assumeTrue(securityClientCert);
        RiakCluster cluster = RiakPemConnection.getRiakCluster("riakuser","riakuser","riakuser-client-cert-key_pkcs8.pem", "riakuser-client-cert.pem");

        for (RiakNode node : cluster.getNodes()){
            assertNotNull(Whitebox.invokeMethod(node, "getConnection", new Object[0]));
        }

        cluster.shutdown();
    }

    @Test
    public void testTrustBasedAuthentication() throws Exception {
        Assume.assumeTrue(security);
        RiakCluster cluster = RiakPemConnection.getRiakCluster("riak_trust_user","riak_trust_user");

        for (RiakNode node : cluster.getNodes()){
            assertNotNull(Whitebox.invokeMethod(node, "getConnection", new Object[0]));
        }

        cluster.shutdown();
    }

    @Test
    public void testPasswordBasedAuthentication() throws Exception {
        Assume.assumeTrue(security);
        RiakCluster cluster = RiakPemConnection.getRiakCluster("riakpass","Test1234");

        for (RiakNode node : cluster.getNodes()){
            assertNotNull(Whitebox.invokeMethod(node, "getConnection", new Object[0]));
        }

        cluster.shutdown();
    }
}
