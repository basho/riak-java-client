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
package com.basho.riak.client.bucket;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.basho.riak.client.builders.BucketPropertiesBuilder;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.raw.RawClient;

/**
 * @author russell
 * 
 */
public class LazyBucketPropertiesTest {

    private static final String BUCKET = "b";
    @Mock private RawClient client;
    @Mock private Retrier retrier;

    private BucketProperties delegate;

    private LazyBucketProperties props;

    /**
     * @throws java.lang.Exception
     */
    @Before public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        delegate = new BucketPropertiesBuilder().nVal(5).build();
        props = new LazyBucketProperties(client, retrier, BUCKET);
    }

    @SuppressWarnings("unchecked") @Test public void lazyLoadsPropertiesOnceOnly() throws Exception {
        final int numThreads = 1000;
        final CountDownLatch endLatch = new CountDownLatch(numThreads);
        final CountDownLatch startLatch = new CountDownLatch(1);

        final PropsRunner[] workers = new PropsRunner[numThreads];

        when(retrier.attempt(any(Callable.class))).thenReturn(delegate);

        for (int i = 0; i < numThreads; i++) {
            workers[i] = new PropsRunner(endLatch, startLatch, props);
            new Thread(workers[i]).start();
        }

        startLatch.countDown();

        boolean timely = endLatch.await(1000, TimeUnit.MILLISECONDS);

        assertTrue("Expected all threads to complete inside 1 second", timely);
        verify(retrier, times(1)).attempt(any(Callable.class));

        for (PropsRunner r : workers) {
            assertEquals("Expected thread to get nval of 5", 5, r.getResult());
        }
    }

    private static final class PropsRunner implements Runnable {
        private final CountDownLatch endLatch;
        private final CountDownLatch startLatch;
        private final LazyBucketProperties props;
        private int result;

        /**
         * @param endLatch
         * @param startLatch
         * @param props
         */
        private PropsRunner(CountDownLatch endLatch, CountDownLatch startLatch, LazyBucketProperties props) {
            this.endLatch = endLatch;
            this.startLatch = startLatch;
            this.props = props;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Runnable#run()
         */
        public void run() {
            try {
                startLatch.await();
                result = props.getNVal();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                result = -1;
            } finally {
                endLatch.countDown();
            }
        }

        public synchronized int getResult() {
            return result;
        }
    }

    @Test public void futureTaskTest() throws Exception {
        final int numThreads = 100;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(numThreads);
        final AtomicInteger cnt = new AtomicInteger(0);

        final FutureTask<Void> ft = new FutureTask<Void>(new Callable<Void>() {

            public Void call() throws Exception {
                cnt.getAndIncrement();
                return null;
            }
        });

        for (int i = 0; i < numThreads; i++) {
            new Thread(new Runnable() {

                public void run() {
                    try {
                        startLatch.await();
                        ft.run();
                        //cnt.getAndIncrement();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        endLatch.countDown();
                    }

                }
            }).start();
        }
        startLatch.countDown();
        boolean timely = endLatch.await(1000, TimeUnit.MILLISECONDS);

        assertTrue("Expected to finish in 1 second", timely);
        assertEquals("Expected only one execution of the callable", 1, cnt.get());
    }
}