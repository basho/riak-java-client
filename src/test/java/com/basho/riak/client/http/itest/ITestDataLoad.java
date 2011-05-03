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
package com.basho.riak.client.http.itest;


import static com.basho.riak.client.http.Hosts.RIAK_URL;
import static org.junit.Assert.*;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import com.basho.riak.client.http.RiakClient;
import com.basho.riak.client.http.RiakObject;

/**
 * Assumes Riak is reachable at {@link com.basho.riak.client.http.Hosts#RIAK_URL }.
 * @see com.basho.riak.client.http.Hosts#RIAK_URL
 */
public class ITestDataLoad {

    final String BUCKET = "test_data_load";
    final int NUM_VALUES = 10;
    final int VALUE_LENGTH = 512;
    
    byte data[][] = new byte[NUM_VALUES][VALUE_LENGTH]; 
    
    @Before public void setup() {
        for (int i = 0; i < 10; i++) {
            new Random().nextBytes(data[i]);
        }
    }
    
    @Test public void concurrent_data_load() throws InterruptedException {
        
        final int NUM_THREADS = 5;
        final int NUM_OBJECTS = 200;
        final Thread[] threads = new Thread[NUM_THREADS];
        final AtomicInteger idx = new AtomicInteger(0);
        
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                
                public void run() {
                    RiakClient riak = new RiakClient(RIAK_URL);
                    Random rnd = new Random();
                    for (int i = 0; i < NUM_OBJECTS / NUM_THREADS; i++) {
                        String key = "data-load-" + idx.getAndIncrement();
                        String value = new String(data[rnd.nextInt(NUM_VALUES)]);
                        RiakObject o = riak.fetch(BUCKET, key).getObject();
                        if (o == null) {
                            o = new RiakObject(riak, BUCKET, key, value.getBytes());
                        } else {
                            o.setValue(value);
                        }
                        Utils.assertSuccess(o.store());
                    }
                }
            });
            threads[i].start();
        }
        
        for (Thread thread : threads) {
            thread.join();
        }
    }

    @Test public void vclock_doesnt_explode_using_single_client() {
        final RiakClient riak = new RiakClient(RIAK_URL);
        final String bucket = UUID.randomUUID().toString();
        final String key = "test-vclock-size";
        final RiakObject o = new RiakObject(riak, bucket, key);
        final Random rnd = new Random();

        Utils.assertSuccess(o.store());

        String originalVclock = o.getVclock();

        for (int i = 0; i < 15; i++) {
            o.fetch();
            o.setValue(data[rnd.nextInt(NUM_VALUES)]);
            Utils.assertSuccess(o.store());
        }

        assertEquals(originalVclock.length(), o.getVclock().length());
        o.delete(Utils.WRITE_3_REPLICAS());
    }
}