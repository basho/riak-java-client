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

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakFutureListener;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.DeleteOperation;
import com.basho.riak.client.core.operations.ListKeysOperation;
import com.basho.riak.client.core.operations.ResetBucketPropsOperation;
import com.basho.riak.client.util.BinaryValue;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public abstract class ITestBase
{
    protected static RiakCluster cluster;
    protected static boolean testYokozuna;
    protected static boolean test2i;
    protected static boolean testBucketType;
    protected static boolean testCrdt;
    protected static boolean legacyRiakSearch;
    protected static boolean security;
    protected static BinaryValue bucketName;
    protected static BinaryValue counterBucketType;
    protected static BinaryValue setBucketType;
    protected static BinaryValue mapBucketType;
    protected static BinaryValue bucketType;
    protected static String overrideCert;

    @BeforeClass
    public static void setUp() throws UnknownHostException, FileNotFoundException, CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException
    {
        /**
         * Riak security.
         * 
         * If you want to test SSL/AUTH you need to:
         *  1) configure riak with the certs included in test/resources and 
         *  2) create a user "tester" with the password "tester"
         *  3) grant all permissions to tester on any:
         *     riak-admin security grant riak_kv.get,riak_kv.put,riak_kv.delete,riak_kv.index,riak_kv.list_keys,riak_kv.list_buckets,riak_core.get_bucket,riak_core.set_bucket,riak_core.get_bucket_type,riak_core.set_bucket_type ON ANY TO tester
         * 
         *  You can override the default cert included in resources using -Dcom.basho.riak.secutiry.cacert=file
         */
        security = Boolean.parseBoolean(System.getProperty("com.basho.riak.security"));
        overrideCert = System.getProperty("com.basho.riak.security.cacert");
        
        testYokozuna = Boolean.parseBoolean(System.getProperty("com.basho.riak.yokozuna"));
        test2i = Boolean.parseBoolean(System.getProperty("com.basho.riak.2i"));
        // You must create a bucket type 'test_type' if you enable this.
        testBucketType = Boolean.parseBoolean(System.getProperty("com.basho.riak.buckettype"));
        testCrdt = Boolean.parseBoolean(System.getProperty("com.basho.riak.crdt"));
        bucketType = BinaryValue.unsafeCreate("test_type".getBytes());
        legacyRiakSearch = Boolean.parseBoolean(System.getProperty("com.basho.riak.riakSearch"));
        bucketName = BinaryValue.unsafeCreate("ITestBase".getBytes());
        
        /**
         * In order to run the CRDT itests you must first manually
         * create the following bucket types in your riak instance
         * with the corresponding bucket properties.
         *
         * maps: allow_mult = true, datatype = asMap
         * sets: allow_mult = true, datatype = asSet
         * counters: allow_mult = true, datatype = counter
         */
        counterBucketType = BinaryValue.create("counters");
        setBucketType = BinaryValue.create("sets");
        mapBucketType = BinaryValue.create("maps");

        RiakNode.Builder builder = new RiakNode.Builder()
                                        .withMinConnections(10);

        if (security)
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
            ks.load(null, "password".toCharArray());
            ks.setCertificateEntry("mycert", caCert);
            
            builder.withAuth("tester", "tester", ks);
        }
        
        
        
        cluster = new RiakCluster.Builder(builder.build()).build();
        cluster.start();
    }
    
    @Before
    public void beforeTest() throws InterruptedException, ExecutionException
    {
        resetAndEmptyBucket(bucketName);
    }
    
    @AfterClass
    public static void tearDown() throws InterruptedException, ExecutionException
    {
        resetAndEmptyBucket(bucketName);
        cluster.shutdown().get();
    }
    
    public static void resetAndEmptyBucket(BinaryValue name) throws InterruptedException, ExecutionException
    {
        resetAndEmptyBucket(name, null);

    }

    protected static void resetAndEmptyBucket(BinaryValue name, BinaryValue type) throws InterruptedException, ExecutionException
    {
        ListKeysOperation.Builder keysOpBuilder = new ListKeysOperation.Builder(name);
        if (type != null)
        {
            keysOpBuilder.withBucketType(type);
        }

        ListKeysOperation keysOp = keysOpBuilder.build();
        cluster.execute(keysOp);
        List<BinaryValue> keyList = keysOp.get();
        final int totalKeys = keyList.size();
        final Semaphore semaphore = new Semaphore(10);
        final CountDownLatch latch = new CountDownLatch(1);
        
        RiakFutureListener<Boolean> listener = new RiakFutureListener<Boolean>() {

            private final AtomicInteger received = new AtomicInteger();
            
            @Override
            public void handle(RiakFuture<Boolean> f)
            {
                try
                {
                    f.get();
                }
                catch (InterruptedException ex)
                {
                    throw new RuntimeException(ex);
                }
                catch (ExecutionException ex)
                {
                    throw new RuntimeException(ex);
                }
                semaphore.release();
                received.incrementAndGet();
                if (received.intValue() == totalKeys)
                {
                    latch.countDown();
                }
            }
            
        };
        
        for (BinaryValue k : keyList)
        {
            DeleteOperation.Builder delOpBuilder = new DeleteOperation.Builder(name, k);
            if (type != null)
            {
                delOpBuilder.withBucketType(type);
            }
            DeleteOperation delOp = delOpBuilder.build();
            delOp.addListener(listener);
            semaphore.acquire();
            cluster.execute(delOp);
        }

        if (!keyList.isEmpty())
        {
            latch.await();
        }
        
        ResetBucketPropsOperation.Builder resetOpBuilder = 
            new ResetBucketPropsOperation.Builder(name);
        if (type != null)
        {
            resetOpBuilder.withBucketType(type);
        }

        ResetBucketPropsOperation resetOp = resetOpBuilder.build();
        cluster.execute(resetOp);
        resetOp.get();

    }
    
}
