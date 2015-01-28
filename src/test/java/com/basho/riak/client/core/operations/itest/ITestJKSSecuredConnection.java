package com.basho.riak.client.core.operations.itest;

import static org.junit.Assert.assertNotNull;

import org.junit.BeforeClass;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;

public class ITestJKSSecuredConnection {

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
         *  5) Import cacert.pem and cert.pem as a Trusted Certs in truststore.jks
         *      $JAVA_HOME/bin/keytool -import -trustcacerts -keystore truststore.jks -file cacert.pem -alias cacert -storepass riak123
         *      $JAVA_HOME/bin/keytool -import -trustcacerts -keystore truststore.jks -file cert.pem -alias servercert -storepass riak123
         *
         *  6) For Certificate Based Authentication, create user certificate and sign it using cacert.pem
         *      $JAVA_HOME/bin/keytool -genkey -dname "CN=riak_cert_user, OU=rjc test, O=The Sample Company, L=Metropolis, S=New York, C=US" -keyalg RSA -alias riak_cert_user -keystore riak_cert_user.jks -storepass riak123 -keypass riak_cert_user -validity 3650 -keysize 2048
         *      $JAVA_HOME/bin/keytool -keystore riak_cert_user.jks -certreq -alias riak_cert_user -keyalg RSA -storepass riak123 -keypass riak_cert_user -file riak_cert_user.csr
         *
         *      openssl x509 -req -days 3650 -in riak_cert_user.csr -CA cacert.pem -CAkey cacert.key -CAcreateserial -out riak_cert_user.pem
         *
         *      $JAVA_HOME/bin/keytool -import -trustcacerts -keystore riak_cert_user.jks -file cacert.pem -alias cacert -storepass riak123
         *      $JAVA_HOME/bin/keytool -import -keystore riak_cert_user.jks -file riak_cert_user.pem -alias riak_cert_user -storepass riak123 -keypass riak_cert_user
         *
         *  7) Run the Test suit
         */
    }

    @Test
    public void testCetificateBasedAuthentication() throws Exception {
        RiakCluster cluster = RiakJKSConnection.getRiakCluster("riak_cert_user","riak_cert_user","riak_cert_user.jks","riak123","riak_cert_user");

        for (RiakNode node : cluster.getNodes()){
            assertNotNull(Whitebox.invokeMethod(node, "getConnection", new Object[0]));
        }

        cluster.shutdown();
    }

    @Test
    public void testTrustBasedAuthentication() throws Exception {
        RiakCluster cluster = RiakJKSConnection.getRiakCluster("riak_trust_user","riak_trust_user");

        for (RiakNode node : cluster.getNodes()){
            assertNotNull(Whitebox.invokeMethod(node, "getConnection", new Object[0]));
        }

        cluster.shutdown();
    }

    @Test
    public void testPasswordBasedAuthentication() throws Exception {
        RiakCluster cluster = RiakJKSConnection.getRiakCluster("riak_passwd_user","riak_passwd_user");

        for (RiakNode node : cluster.getNodes()){
            assertNotNull(Whitebox.invokeMethod(node, "getConnection", new Object[0]));
        }

        cluster.shutdown();
    }
}
