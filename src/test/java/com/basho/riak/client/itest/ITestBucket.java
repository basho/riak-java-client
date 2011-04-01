/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.client.itest;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.*;

import org.junit.Test;

import com.basho.riak.newapi.RiakClient;
import com.basho.riak.newapi.RiakException;
import com.basho.riak.newapi.RiakFactory;
import com.basho.riak.newapi.RiakObject;
import com.basho.riak.newapi.RiakRetryFailedException;
import com.basho.riak.newapi.bucket.Bucket;
import com.basho.riak.newapi.cap.UnresolvedConflictException;

/**
 * @author russell
 * 
 */
public class ITestBucket {

    @Test public void basicStore() throws Exception {
        final String bucketName = UUID.randomUUID().toString();
        RiakClient c = RiakFactory.pbcClient();

        Bucket b = c.fetchBucket(bucketName).execute();
        RiakObject o = b.store("k", "v").execute();
        assertNull(o);
    }

    @Test public void siblings() throws Exception {
        final CountDownLatch cdl = new CountDownLatch(1);
        final String bucketName = UUID.randomUUID().toString();

        RiakFactory.pbcClient().createBucket(bucketName).allowSiblings(true).execute();

        final int numThreads = 2;
        final Thread[] threads = new Thread[numThreads];

        CountDownLatch el = new CountDownLatch(numThreads);

        for (int i = 0; i < numThreads; i++) {
            RiakClient c = RiakFactory.pbcClient();
            c.generateAndSetClientId();
            threads[i] = new Thread(new Storer(cdl, el, c.fetchBucket(bucketName).execute(), "k", "v"));
            threads[i].start();
        }

        cdl.countDown();

        el.await();
        System.out.println(bucketName);
    }

    private static final class Storer implements Runnable {
        private final CountDownLatch startLatch;
        private final CountDownLatch endLatch;
        private final Bucket bucket;
        private final String key;
        private final String value;

        /**
         * @param startLatch
         * @param bucket
         * @param key
         * @param value
         */
        private Storer(CountDownLatch startLatch, CountDownLatch endLatch, Bucket bucket, String key, String value) {
            this.startLatch = startLatch;
            this.endLatch = endLatch;
            this.bucket = bucket;
            this.key = key;
            this.value = value;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Runnable#run()
         */
        public void run() {
            try {
                startLatch.await();
                for (int i = 0; i < 5; i++) {
                    System.out.println(Thread.currentThread().getName() + " doing run " + i);
                    bucket.store(key, Thread.currentThread().getName() + value + i).execute();
                    Thread.sleep(10);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (RiakException e) {
                System.out.println(Thread.currentThread().getName() + " just barfed");
                throw new RuntimeException(e);
            } finally {
                endLatch.countDown();
            }
        }

    }

}
