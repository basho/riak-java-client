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

import static com.basho.riak.client.AllTests.emptyBucket;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.operations.FetchObject;
import com.basho.riak.client.raw.StreamingOperation;
import com.basho.riak.client.util.CharsetUtils;
import java.util.HashSet;

/**
 * @author russell
 * 
 */
public abstract class ITestClientBasic {

    protected IRiakClient client;
    protected String bucketName;

    @Before public void setUp() throws RiakException {
        this.client = getClient();
        this.bucketName = this.getClass().getName();
        emptyBucket(bucketName, client);
    }

    /**
     * @return an {@link IRiakClient} implementation for the transport to be
     *         tested.
     */
    protected abstract IRiakClient getClient() throws RiakException;

    @Test public void ping() throws RiakException {
        client.ping();
    }

    @Test public void fetchBucket() throws RiakException {
        Bucket b = client.fetchBucket(bucketName).execute();

        assertNotNull(b);
        assertEquals(bucketName, b.getName());
        assertEquals(new Integer(3), b.getNVal());
        assertFalse(b.getAllowSiblings());
    }

    @Test public void updateBucket() throws RiakException {
        Bucket b = client.fetchBucket(bucketName).execute();

        assertNotNull(b);
        assertEquals(bucketName, b.getName());
        assertEquals(new Integer(3), b.getNVal());
        assertFalse(b.getAllowSiblings());

        b = client.updateBucket(b).nVal(4).allowSiblings(true).execute();

        assertNotNull(b);
        assertEquals(bucketName, b.getName());
        assertEquals(new Integer(4), b.getNVal());
        assertTrue(b.getAllowSiblings());

        client.updateBucket(b).allowSiblings(false).nVal(3).execute();
    }

    @Test public void createBucket() throws RiakException {
        Bucket b = client.createBucket(bucketName).nVal(1).allowSiblings(true).execute();

        assertNotNull(b);
        assertEquals(bucketName, b.getName());
        assertEquals(new Integer(1), b.getNVal());
        assertTrue(b.getAllowSiblings());
        client.updateBucket(b).allowSiblings(false).nVal(3).execute();
    }

    @Test public void clientIds() throws Exception {
        final byte[] clientId = CharsetUtils.utf8StringToBytes("abcd");

        client.setClientId(clientId.clone());
        assertArrayEquals(clientId, client.getClientId());

        byte[] newId = client.generateAndSetClientId();

        assertArrayEquals(newId, client.getClientId());
    }

    @Test public void listBuckets() throws RiakException {
        final String bucket1 = bucketName + "_1";
        final String bucket2 = bucketName + "_2";

        Bucket b1 = client.createBucket(bucket1).execute();
        Bucket b2 = client.createBucket(bucket2).execute();

        b1.store("key", "value").execute();
        b2.store("key", "value").execute();

        // Non-streaming
        Set<String> buckets = client.listBuckets();

        assertTrue("Expected bucket 1 to be present", buckets.contains(bucket1));
        assertTrue("Expected bucket 2 to be present", buckets.contains(bucket2));

        // Streaming
        StreamingOperation<String> sOperation = client.listBucketsStreaming();
        buckets = new HashSet<String>();
        while (sOperation.hasNext()) {
            buckets.add(sOperation.next());
        }
        
        assertTrue("Expected bucket 1 to be present", buckets.contains(bucket1));
        assertTrue("Expected bucket 2 to be present", buckets.contains(bucket2));
        
        emptyBucket(bucket2, client);
        emptyBucket(bucket1, client);
    }

    /**
     * Each transport implementation has its own set of supported properties.
     * @throws Exception
     */
    @Test public abstract void bucketProperties() throws Exception;

    @Test public void conditionalFetch() throws Exception {
        final String key = "key";
        final String originalValue = "first_value";
        final String newValue = "second_value";

        Bucket b = client.fetchBucket(bucketName).execute();
        b.store(key, originalValue).execute();

        IRiakObject obj = b.fetch(key).execute();

        assertNotNull(obj);
        assertEquals(originalValue, obj.getValueAsString());

        final FetchObject<IRiakObject> fo = b.fetch(key)
            .ifModified(obj.getVClock()) // in case of PB
            .modifiedSince(obj.getLastModified()); // in case of HTTP

        IRiakObject obj2 = fo.execute();

        assertNull(obj2);
        assertTrue(fo.isUnmodified());

        // wait because of coarseness of last modified time update (HTTP).
        Thread.sleep(1000);
        // change it, fetch it
        obj.setValue(newValue);
        b.store(obj).execute();

        IRiakObject obj3 = b.fetch(key)
            .ifModified(obj.getVClock()) // in case of PB
            .modifiedSince(obj.getLastModified()) // in case of HTTP
            .execute();

        assertNotNull(obj3);
        assertEquals(newValue, obj3.getValueAsString());
    }

    @Test public void deletedVclock() throws Exception {
        final String key = "k";

        final Bucket b = client.fetchBucket(bucketName).execute();

        final CountDownLatch endLatch = new CountDownLatch(1);

        final Runnable putter = new Runnable() {

            public void run() {
                int cnt = 0;
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        b.store(key, String.valueOf(cnt)).execute();
                    } catch (RiakException e) {
                        // no-op, keep going
                    }
                }
            }
        };

        final Thread putterThread = new Thread(putter);

        final Runnable deleter = new Runnable() {

            public void run() {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        b.delete(key).execute();
                    } catch (RiakException e) {
                        // no-op, keep going
                    }
                }
            }
        };

        final Thread deleterThread = new Thread(deleter);

        final Runnable getter = new Runnable() {
            public void run() {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        FetchObject<IRiakObject> fo = b.fetch(key).returnDeletedVClock(true);
                        IRiakObject o = fo.execute();

                        if (o != null && o.isDeleted()) {
                            endLatch.countDown();
                            Thread.currentThread().interrupt();
                            putterThread.interrupt();
                            deleterThread.interrupt();
                        }
                    } catch (RiakException e) {
                        // no-op, keep going
                    }
                }
            }
        };

        final Thread getterThread = new Thread(getter);

        putterThread.start();
        deleterThread.start();
        getterThread.start();

        boolean sawDeletedVClock = endLatch.await(10, TimeUnit.SECONDS);
        
        if (!sawDeletedVClock) {
            putterThread.interrupt();
            getterThread.interrupt();
            deleterThread.interrupt();
        } 
        
        // There's a *slight* chance this test may fail on a really slow 
        // machine but otherwise it should be true.
        assertTrue(sawDeletedVClock); 
        
    }
    
    @Test public void deletedVclockSiblings() throws Exception {
        
        final String key = "j";
        final String bucket2 = bucketName + "_dsiblings";
        final Bucket b = client.createBucket(bucket2).allowSiblings(true).execute();
        
        b.store(key,"something").execute();
        b.delete(key).execute();
        b.store(key,"something").execute();
        
        // We will now have siblings, once of which is a tombstone. Fetching 
        // the key back while setting returnDeletedVClock() should give us 
        // siblings, whereas without it we should only get back the non-deleted
        // object
        
        IRiakObject ro = b.fetch(key).execute();
        assertNotNull(ro);
        assertEquals(ro.getValueAsString(), "something");
        
        try {
            ro = b.fetch(key).returnDeletedVClock(true).execute();
            fail("Expected siblings to exist, including deleted vclock");
        } catch (UnresolvedConflictException e) {
            // no-op this is what we expect 
        }
        
        emptyBucket(bucket2, client);
        
        
    }
}
