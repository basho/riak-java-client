package com.basho.riak.client.core;

import com.basho.riak.client.core.util.Constants;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;


public class SSLEngineTests
{
    private final String[] enabledCiphers;
    private final KeyStore trustStore;
    private final boolean security;
    private final Integer pbcPort;

    public SSLEngineTests() throws NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException,
            KeyManagementException
    {
        security = Boolean.parseBoolean(System.getProperty("com.basho.riak.security"));

        if(!security)
        {
            enabledCiphers = null;
            trustStore = null;
            pbcPort = 0;
            return;
        }

        pbcPort = Integer.getInteger("com.basho.riak.pbcport", RiakNode.Builder.DEFAULT_REMOTE_PORT);

        this.trustStore = getTrustStore();

        final SSLContext context = SSLContext.getInstance("TLS");

        TrustManagerFactory tmf =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);

        context.init(null, tmf.getTrustManagers(), null);

        SSLEngine engine = context.createSSLEngine();

        this.enabledCiphers = engine.getEnabledCipherSuites();
    }

    private static KeyStore getTrustStore()
            throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException
    {
        CertificateFactory cFactory = CertificateFactory.getInstance("X.509");

        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("cacert.pem");
        X509Certificate caCert = (X509Certificate) cFactory.generateCertificate(in);
        in.close();

        in = Thread.currentThread().getContextClassLoader().getResourceAsStream("riak-test-cert.pem");
        X509Certificate serverCert = (X509Certificate) cFactory.generateCertificate(in);
        in.close();

        final KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, "basho".toCharArray());
        trustStore.setCertificateEntry("cacert", caCert);
        trustStore.setCertificateEntry("server", serverCert);
        return trustStore;
    }

    @Test
    public void VerifyCurrentCipherListSupport() throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException
    {
        Assume.assumeTrue(security);
        Set<String> r16Ciphers = new LinkedHashSet<>(Arrays.asList(Constants.SUPPORTED_RIAK_R16_CIPHERS));

        Set<String> localSupported = new LinkedHashSet<>();
        Set<String> localUnsupported = new LinkedHashSet<>();

        testAllLocalCiphers(localSupported, localUnsupported);

        localSupported.removeAll(r16Ciphers);

        assertEquals("Ciphers found that are supported on Riak, but not listed as such internally ", 0, localSupported.size());
    }

    @Test
    @Ignore("Run to get the current platform's list of supported ciphers.")
    public void PrintSupportedLocalCipherList()
            throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException
    {
        Assume.assumeTrue(security);
        Set<String> r16Ciphers = new LinkedHashSet<>(Arrays.asList(Constants.SUPPORTED_RIAK_R16_CIPHERS));

        Set<String> localSupported = new LinkedHashSet<>();
        Set<String> localUnsupported = new LinkedHashSet<>();

        testAllLocalCiphers(localSupported, localUnsupported);

        System.out.println("JAVA VERSION: " + System.getProperty("java.version"));

        System.out.println("\nLocal Supported Ciphers:");
        for (String s : localSupported)
        {
            System.out.println(s);
        }

        System.out.println("\nLocal Unsupported Ciphers: ");
        for (String s : localUnsupported)
        {
            System.out.println(s);
        }

        r16Ciphers.removeAll(localSupported);

        System.out.println("\nUnsupported R16 Ciphers:");

        for (String r16Cipher : r16Ciphers)
        {
            System.out.println(r16Cipher);
        }
    }

    private void testAllLocalCiphers(Set<String> localSupported, Set<String> localUnsupported)
            throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException
    {
        for (String enabledCipher : enabledCiphers)
        {
            RiakNode node = new RiakNode.Builder().withAuth("riakpass", "Test1234", trustStore)
                                                  .withRemotePort(pbcPort).build();

            Whitebox.setInternalState(node, "commonSupportedSSLCiphers",
                                      new AtomicReference<>(new String[]{enabledCipher}));

            node.start();

            final LinkedBlockingDeque<Object> available = Whitebox.getInternalState(node, "available");
            final int i = available.size();
            if(i == 0)
            {
                localUnsupported.add(enabledCipher);
            }
            else
            {
                localSupported.add(enabledCipher);
            }
        }
    }
}
