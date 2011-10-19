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

import static com.basho.riak.client.http.Hosts.RIAK_HOST;
import static com.basho.riak.client.http.Hosts.RIAK_PORT;
import static com.google.protobuf.ByteString.copyFrom;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import com.basho.riak.client.util.CharsetUtils;
import com.basho.riak.pbc.RequestMeta;
import com.basho.riak.pbc.RiakClient;
import com.basho.riak.pbc.RiakObject;
import com.basho.riak.test.util.ExpectedValues;
import com.google.protobuf.ByteString;

/**
 * Assumes Riak is reachable at {@link com.basho.riak.client.http.Hosts#RIAK_HOST }.
 * @see com.basho.riak.client.http.Hosts#RIAK_HOST
 */
public class ITestDataLoad {

    private static final String BUCKET = "__itest_java_pbc_dataload__";
    final int NUM_VALUES = 10;
    final int VALUE_LENGTH = 512;

    byte data[][] = new byte[NUM_VALUES][VALUE_LENGTH];

    @Before public void setup() {
        for (int i = 0; i < 10; i++) {
            new Random().nextBytes(data[i]);
        }
    }

    @Test public void multiDataLoad() throws Exception {
        final String bucket = UUID.randomUUID().toString();
        final int NUM_OBJECTS = 200;
        int idx = 0;
        final RiakClient riak = new RiakClient(RIAK_HOST, RIAK_PORT);
        riak.setClientID("PMDL");
        final RiakObject[] objects = new RiakObject[NUM_OBJECTS];
        final RequestMeta meta = new RequestMeta();
        meta.w(3);
        meta.dw(2);

        Random rnd = new Random();
        for (int i = 0; i < NUM_OBJECTS; i++) {
            String key = "data-load-" + idx;
            String value = CharsetUtils.asString(data[rnd.nextInt(NUM_VALUES)], CharsetUtils.ISO_8859_1);;
            RiakObject o = new RiakObject(bucket, key, value);
            objects[i] = o;
            idx++;
        }

        final List<ByteString> vclocks = Arrays.asList(riak.store(objects, meta));

        assertEquals(NUM_OBJECTS, vclocks.size());

        for (RiakObject o : objects) {
            RiakObject[] fetched = riak.fetch(bucket, o.getKey());
            assertEquals(1, fetched.length);
            RiakObject o2 = fetched[0];
            assertEquals("No match for " + o.getKey(), o.getValue().toStringUtf8(), o2.getValue().toStringUtf8());
            assertTrue("Vclock for " +o2.getKey() + " not present in vclocks array " + o2.getVclock(), vclocks.contains(o2.getVclock()));
        }

        for(ByteString key : riak.listKeys(copyFromUtf8(bucket))) {
            riak.delete(bucket, key.toStringUtf8());
        }
    }

    @Test public void concurrentDataLoad() throws Exception {
        final int NUM_THREADS = 5;
        final int NUM_OBJECTS = 200;

        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(NUM_THREADS);

        final Thread[] threads = new Thread[NUM_THREADS];
        final AtomicInteger idx = new AtomicInteger(0);

        final RiakClient riak = new RiakClient(RIAK_HOST, RIAK_PORT);

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {

                public void run() {
                    try {
                        startLatch.await();

                        Random rnd = new Random();
                        for (int i = 0; i < NUM_OBJECTS / NUM_THREADS; i++) {
                            String key = "data-load-" + idx.getAndIncrement();
                            String value = CharsetUtils.asString(data[rnd.nextInt(NUM_VALUES)], CharsetUtils.ISO_8859_1);
                            RiakObject[] objects = riak.fetch(BUCKET, key);
                            RiakObject o = null;
                            if (objects.length == 0) {
                                o = new RiakObject(BUCKET, key, value);
                            } else {
                                o = new RiakObject(objects[0].getVclock(), objects[0].getBucketBS(),
                                                   objects[0].getKeyBS(), copyFromUtf8(value));
                            }

                            RiakObject result = riak.store(o, new RequestMeta().w(2).returnBody(true))[0];
                            assertEquals(o.getBucket(), result.getBucket());
                            assertEquals(o.getKey(), result.getKey());
                            assertEquals(o.getValue(), result.getValue());
                        }

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        endLatch.countDown();
                    }
                }
            });
            threads[i].start();
        }

        startLatch.countDown();

        endLatch.await();
    }

    @Test public void vclockDoesntExplodeUsingSingleClient() throws Exception {
        final RiakClient c = new RiakClient(RIAK_HOST, RIAK_PORT);
        c.setClientID("TVCS");
        final Random rnd = new Random();
        final String bucket = UUID.randomUUID().toString();
        final String key = "pbc-test-vclock-size";

        RiakObject o = new RiakObject(bucket, key, ExpectedValues.CONTENT);

        ByteString originalVclock = c.store(o, new RequestMeta().w(3).returnBody(true))[0].getVclock();

        for (int i = 0; i < 15; i++) {
            o = c.fetch(o.getBucketBS(), o.getKeyBS())[0];
            o = new RiakObject(o.getVclock(), o.getBucketBS(), o.getKeyBS(), copyFrom(data[rnd.nextInt(NUM_VALUES)]));
            c.store(o);
        }

        assertEquals(originalVclock.size(), o.getVclock().size());
        c.delete(bucket, key);
    }
}