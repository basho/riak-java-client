package com.basho.riak.client.core.operations.itest;

import static org.junit.Assert.assertNotNull;

import org.junit.BeforeClass;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;

public class ITestPemSecuredConnection {

    @BeforeClass
    public static void setUp() {

        /**
         * Riak security.
         *
         * If you want to test SSL/AUTH, you need to:
         *  1) configure riak with the certs included in test/resources
         *      ssl.certfile = $(platform_etc_dir)/cert.pem
         *      ssl.keyfile = $(platform_etc_dir)/key.pem
         *      ssl.cacertfile = $(platform_etc_dir)/cacert.pem
         *
         *  2) create a user "riak_cert_user" with the password "riak_cert_user" and configure it with certificate as a source
         *      riak-admin security add-user riak_cert_user password=riak_cert_user
         *      riak-admin security add-source riak_cert_user 0.0.0.0/0 certificate
         *
         *  3) create a user "riak_trust_user" with the password "riak_trust_user" and configure it with trust as a source
         *      riak-admin security add-user riak_trust_user password=riak_trust_user
         *      riak-admin security add-source riak_trust_user 0.0.0.0/0 trust
         *
         *  4) create a user "riak_passwd_user" with the password "riak_passwd_user" and configure it with password as a source
         *      riak-admin security add-user riak_passwd_user password=riak_passwd_user
         *      riak-admin security add-source riak_passwd_user 0.0.0.0/0 password
         *
         *  5) For Certificate Based Authentication, create user certificate and sign it using cacert.pem
         *      openssl genrsa -out riak_cert_user_key.pem 2048
         *      openssl req -new -key riak_cert_user_key.pem -out riak_cert_user.csr -subj "/C=US/ST=New York/L=Metropolis/O=The Sample Company/OU=rjc test/CN=riak_cert_user/emailAddress=riak_cert_user@basho.com"
         *
         *      #Sign the cert with CA.
         *      openssl x509 -req -days 3650 -in riak_cert_user.csr -CA cacert.pem -CAkey cacert.key -CAcreateserial -out riak_cert_user_cert.pem
         *
         *      openssl pkcs8 -topk8 -inform PEM -outform PEM -in riak_cert_user_key.pem -out riak_cert_user_key_pkcs8.pem -nocrypt
         *
         *  6) Run the Test suit
         */
    }

    @Test
    public void testCetificateBasedAuthentication() throws Exception {
        RiakCluster cluster = RiakPemConnection.getRiakCluster("riak_cert_user","riak_cert_user","riak_cert_user_key_pkcs8.pem", "riak_cert_user_cert.pem");

        for (RiakNode node : cluster.getNodes()){
            assertNotNull(Whitebox.invokeMethod(node, "getConnection", new Object[0]));
        }

        cluster.shutdown();
    }

    @Test
    public void testTrustBasedAuthentication() throws Exception {
        RiakCluster cluster = RiakPemConnection.getRiakCluster("riak_trust_user","riak_trust_user");

        for (RiakNode node : cluster.getNodes()){
            assertNotNull(Whitebox.invokeMethod(node, "getConnection", new Object[0]));
        }

        cluster.shutdown();
    }

    @Test
    public void testPasswordBasedAuthentication() throws Exception {
        RiakCluster cluster = RiakPemConnection.getRiakCluster("riak_passwd_user","riak_passwd_user");

        for (RiakNode node : cluster.getNodes()){
            assertNotNull(Whitebox.invokeMethod(node, "getConnection", new Object[0]));
        }

        cluster.shutdown();
    }
}
