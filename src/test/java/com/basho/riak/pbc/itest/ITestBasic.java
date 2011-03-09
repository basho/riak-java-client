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
package com.basho.riak.pbc.itest;

import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import com.basho.riak.pbc.RiakClient;

/**
 * @author russell
 *
 */
public class ITestBasic {

    private static final String RIAK_HOST = "127.0.0.1";
    private static final String CLIENT_ID = "__itest_java_pbc_client__";

    @Test public void setClientId() throws Exception {
        final RiakClient riakClient = new RiakClient(RIAK_HOST);
        riakClient.setClientID(CLIENT_ID);

        Thread.sleep(1500);

        riakClient.getClientID();
    }


    @Test public void setClientIdThreaded() throws Exception {
        final RiakClient riakClient = new RiakClient(RIAK_HOST);
        final int numThreads = 10;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(numThreads);
        final Worker[] workers =  new Worker[numThreads];

        for(int i=0 ; i < numThreads; i++) {
            workers[i] = new Worker(startLatch, endLatch, riakClient);
            new Thread(workers[i]).start();
        }

        startLatch.countDown();
        endLatch.await();
    }

    private static final class Worker implements Runnable {

        private final CountDownLatch startLatch;
        private final CountDownLatch endLatch;
        private final RiakClient riakClient;

        public Worker(CountDownLatch startLatch, CountDownLatch endLatch, RiakClient riakClient) {
            super();
            this.startLatch = startLatch;
            this.endLatch = endLatch;
            this.riakClient = riakClient;
        }

        /* (non-Javadoc)
         * @see java.lang.Runnable#run()
         */
        public void run() {
            try {
                startLatch.await();
                riakClient.setClientID(CLIENT_ID);

                Thread.sleep(1500);

                riakClient.getClientID();
            } catch (Exception e) {
               throw new RuntimeException(e);
            } finally {
                endLatch.countDown();
            }
        }

    }

}
