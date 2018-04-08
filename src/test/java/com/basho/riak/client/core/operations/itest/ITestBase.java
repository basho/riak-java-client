/*
 * Copyright 2013 Basho Technologies Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.core.operations.itest;

import com.basho.riak.client.RiakTestFunctions;
import com.basho.riak.client.api.ListException;
import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.ListKeys;
import com.basho.riak.client.api.commands.kv.MultiDelete;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.netty.RiakResponseException;
import com.basho.riak.client.core.operations.*;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.indexes.LongIntIndex;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public abstract class ITestBase extends RiakTestFunctions
{
    protected static RiakCluster cluster;
    protected static boolean testYokozuna;
    protected static boolean test2i;
    protected static boolean testBucketType;
    protected static boolean testCrdt;
    protected static boolean testHllDataType;
    protected static boolean testGSetDataType;
    protected static boolean testTimeSeries;
    protected static boolean testCoveragePlan;
    protected static boolean legacyRiakSearch;
    protected static boolean security;
    protected static BinaryValue bucketName;
    protected static BinaryValue counterBucketType;
    protected static BinaryValue setBucketType;
    protected static BinaryValue gsetBucketType;
    protected static BinaryValue hllBucketType;
    protected static BinaryValue mapBucketType;
    protected static BinaryValue bucketType;
    protected static BinaryValue yokozunaBucketType;
    protected static BinaryValue mapReduceBucketType;
    protected static BinaryValue plainBucketType;
    protected static String overrideCert;
    protected static final int NUMBER_OF_PARALLEL_REQUESTS = 10;
    protected static final int NUMBER_OF_TEST_VALUES = 100;

    protected static String hostname;
    protected static int pbcPort;
    @Rule
    public TestName testName = new TestName();

    @BeforeClass
    public static void setUp()
            throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException, InterruptedException
    {
        bucketName = BinaryValue.unsafeCreate("ITestBase".getBytes());

        /**
         * Riak security.
         *
         * If you want to test SSL/AUTH, you need to set up your system as described
         * in the README.md's "Security Tests" section.
         */

        security = Boolean.parseBoolean(System.getProperty("com.basho.riak.security", "false"));
        overrideCert = System.getProperty("com.basho.riak.security.cacert");

        /**
         * Yokozuna.
         *
         * You need to create a bucket type in Riak for YZ:
         *
         * riak-admin bucket-type create yokozuna '{"props":{}}'
         * riak-admin bucket-type activate yokozuna
         */
        yokozunaBucketType = BinaryValue.create("yokozuna");
        testYokozuna = Boolean.parseBoolean(System.getProperty("com.basho.riak.yokozuna", "true"));

        /**
         * Bucket type
         *
         * you must create the type 'plain' to use this:
         *
         * riak-admin bucket-type create plain '{"props":{}}'
         * riak-admin bucket-type activate plain
         */
        testBucketType = Boolean.parseBoolean(System.getProperty("com.basho.riak.buckettype", "true"));
        bucketType = BinaryValue.unsafeCreate("plain".getBytes());

        /**
         * Secondary indexes
         *
         * The backend must be 'leveldb' in riak config to us this
         */
        test2i = Boolean.parseBoolean(System.getProperty("com.basho.riak.2i", "true"));

        legacyRiakSearch = Boolean.parseBoolean(System.getProperty("com.basho.riak.riakSearch", "false"));

        /**
         * In order to run the CRDT itests you must first manually
         * create the following bucket types in your riak instance
         * with the corresponding bucket properties.
         *
         * riak-admin bucket-type create maps '{"props":{"allow_mult":true, "datatype": "map"}}'
         * riak-admin bucket-type create sets '{"props":{"allow_mult":true, "datatype": "set"}}'
         * riak-admin bucket-type create gsets '{"props":{"allow_mult":true, "datatype": "gset"}}'
         * riak-admin bucket-type create counters '{"props":{"allow_mult":true, "datatype": "counter"}}'
         * riak-admin bucket-type create hlls '{"props":{"allow_mult":true, "datatype": "hll"}}'
         * riak-admin bucket-type activate maps
         * riak-admin bucket-type activate sets
         * riak-admin bucket-type activate gsets
         * riak-admin bucket-type activate counters
         * riak-admin bucket-type activate hlls
         */
        counterBucketType = BinaryValue.create("counters");
        setBucketType = BinaryValue.create("sets");
        gsetBucketType = BinaryValue.create("gsets");
        hllBucketType = BinaryValue.create("hlls");
        mapBucketType = BinaryValue.create("maps");

        mapReduceBucketType = BinaryValue.create("mr");

        plainBucketType = BinaryValue.create("plain");

        testCrdt = Boolean.parseBoolean(System.getProperty("com.basho.riak.crdt", "true"));

        testTimeSeries = Boolean.parseBoolean(System.getProperty("com.basho.riak.timeseries", "false"));

        testCoveragePlan = Boolean.parseBoolean(System.getProperty("com.basho.riak.coveragePlan", "false"));

        /**
         * Riak PBC host
         *
         * In case you want/need to use a custom PBC host you may pass it by using the following system property
         */
        hostname = System.getProperty("com.basho.riak.host", RiakNode.Builder.DEFAULT_REMOTE_ADDRESS);

        /**
         * Riak PBC port
         *
         * In case you want/need to use a custom PBC port you may pass it by using the following system property
         */
        pbcPort = Integer.getInteger("com.basho.riak.pbcport", RiakNode.Builder.DEFAULT_REMOTE_PORT);

        RiakNode.Builder builder = new RiakNode.Builder()
                                        .withRemoteAddress(hostname)
                                        .withRemotePort(pbcPort)
                                        .withMinConnections(NUMBER_OF_PARALLEL_REQUESTS);

        if (security)
        {
            setupUsernamePasswordSecurity(builder);
        }

        cluster = new RiakCluster.Builder(builder.build()).build();
        cluster.start();

        testHllDataType = testCrdt && checkExistanceOfBucketType(hllBucketType);
        testGSetDataType = testCrdt && checkExistanceOfBucketType(gsetBucketType);
    }

    private static boolean checkExistanceOfBucketType(BinaryValue bucketType) throws InterruptedException
    {
        final Namespace ns = new Namespace(bucketType, bucketType);
        final FetchBucketPropsOperation fetchBucketPropsOp = new FetchBucketPropsOperation.Builder(ns).build();

        final RiakFuture<FetchBucketPropsOperation.Response, Namespace>
                bucketPropsFuture = cluster.execute(fetchBucketPropsOp);

        bucketPropsFuture.await();
        return bucketPropsFuture.isSuccess();
    }

    @AfterClass
    public static void tearDown() throws InterruptedException, ExecutionException, TimeoutException
    {
        cluster.shutdown().get(2, TimeUnit.SECONDS);
    }

    public static void resetAndEmptyBucket(BinaryValue name) throws InterruptedException, ExecutionException
    {
        resetAndEmptyBucket(new Namespace(Namespace.DEFAULT_BUCKET_TYPE, name.toString()));
    }

    protected static void resetAndEmptyBucket(Namespace namespace) throws InterruptedException, ExecutionException
    {
        RiakClient client = new RiakClient(cluster);

        ListKeys.Response listKeyResponse = null;
        try
        {
            ListKeys listKeys = new ListKeys.Builder(namespace).withAllowListing().build();
            listKeyResponse = client.execute(listKeys);
        }
        catch (ListException ex)
        {
            fail(ex.getMessage());
        }

        MultiDelete multiDelete = new MultiDelete.Builder().addLocations(listKeyResponse).build();
        final MultiDelete.Response deleteResponse = client.execute(multiDelete);

        ResetBucketPropsOperation.Builder resetOpBuilder =
                new ResetBucketPropsOperation.Builder(namespace);

        ResetBucketPropsOperation resetOp = resetOpBuilder.build();
        cluster.execute(resetOp);
        resetOp.get();
    }

    protected static void setupUsernamePasswordSecurity(RiakNode.Builder builder)
            throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException
    {
        InputStream in;
        if (overrideCert != null)
        {
            File f = new File(overrideCert);
            in = new FileInputStream(f);
        }
        else
        {
            in =
                Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream("cacert.pem");
        }

        CertificateFactory cFactory = CertificateFactory.getInstance("X.509");
        X509Certificate caCert = (X509Certificate) cFactory.generateCertificate(in);
        in.close();

        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, "basho".toCharArray());
        ks.setCertificateEntry("cacert", caCert);

        builder.withAuth("riakpass", "Test1234", ks);
    }

    protected void setBucketNameToTestName()
    {
        bucketName = BinaryValue.create(testName.getMethodName());
    }

    public static boolean assureIndexExists(String indexName) throws InterruptedException
    {
        for (int x = 0; x < 5; x++)
        {
            Thread.sleep(2000);
            YzFetchIndexOperation fetch = new YzFetchIndexOperation.Builder().withIndexName(indexName).build();
            cluster.execute(fetch);
            fetch.await();
            if (fetch.isSuccess())
            {
                return true;
            }
        }

        return false;
    }

    public static Namespace defaultNamespace()
    {
        return new Namespace(testBucketType ?
                                     bucketType :
                                     BinaryValue.createFromUtf8(Namespace.DEFAULT_BUCKET_TYPE), bucketName);
    }

    protected static void assertFutureSuccess(RiakFuture<?, ?> resultFuture)
    {
        if (resultFuture.cause() == null)
        {
            assertTrue(resultFuture.isSuccess());
        }
        else
        {
            assertTrue(resultFuture.cause().getMessage(), resultFuture.isSuccess());
        }
    }

    protected static void assertFutureFailure(RiakFuture<?,?> resultFuture)
    {
        assertFalse(resultFuture.isSuccess());
        assertEquals(resultFuture.cause().getClass(), RiakResponseException.class);
    }

    protected static void setupIndexTestData(Namespace ns, String indexName, String keyBase, String value)
            throws InterruptedException, ExecutionException
    {
        for (long i = 0; i < NUMBER_OF_TEST_VALUES; ++i)
        {
            RiakObject obj = new RiakObject()
                    .setValue(BinaryValue.create(value +i))
                    .setContentType("plain/text");

            obj.getIndexes().getIndex(LongIntIndex.named(indexName)).add(i);

            Location location = new Location(ns, keyBase + i);
            StoreOperation storeOp =
                    new StoreOperation.Builder(location)
                            .withContent(obj)
                            .build();

            cluster.execute(storeOp);
            storeOp.get();
        }
    }
}
