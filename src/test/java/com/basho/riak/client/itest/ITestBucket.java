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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertArrayEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.RiakTestProperties;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.cap.Mutation;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.RiakKey;
import com.basho.riak.client.http.util.Constants;
import com.basho.riak.client.operations.FetchObject;
import com.basho.riak.client.query.indexes.BinIndex;
import com.basho.riak.client.query.indexes.BucketIndex;
import com.basho.riak.client.query.indexes.IntIndex;
import com.basho.riak.client.query.indexes.KeyIndex;
import com.basho.riak.client.raw.MatchFoundException;
import com.basho.riak.client.raw.ModifiedException;

/**
 * @author russell
 * 
 */
public abstract class ITestBucket {

    protected IRiakClient client;
    protected String bucketName;

    @Before public void setUp() throws RiakException, InterruptedException {
        client = getClient();
        bucketName = this.getClass().getName();
        emptyBucket(bucketName, client);
    }

    protected abstract IRiakClient getClient() throws RiakException;

    @Test public void basicStore() throws Exception {
        Bucket b = client.fetchBucket(bucketName).execute();
        IRiakObject o = b.store("k", "v").execute();
        assertNull(o);

        IRiakObject fetched = b.fetch("k").execute();
        assertEquals("v", fetched.getValueAsString());

        // now update that riak object
        b.store("k", "my new value").execute();
        fetched = b.fetch("k").execute();
        assertEquals("my new value", fetched.getValueAsString());
        assertEquals(Constants.CTYPE_TEXT_UTF8, fetched.getContentType());

        // add links and user meta
        final RiakLink link1 = new RiakLink("b", "k2", "brother");
        final RiakLink link2 = new RiakLink("b", "k3", "sister");
        fetched.addLink(link1).addLink(link2);
        fetched.addUsermeta("meta1", "metaValue1").addUsermeta("meta2", "metaValue2");
        fetched.setContentType(Constants.CTYPE_JSON);

        b.store(fetched).execute();

        IRiakObject reFetched = b.fetch("k").execute();

        assertEquals(2, reFetched.numLinks());
        assertTrue(reFetched.hasLink(link1));
        assertTrue(reFetched.hasLink(link2));

        assertEquals("metaValue1", reFetched.getUsermeta("meta1"));
        assertEquals("metaValue2", reFetched.getUsermeta("meta2"));

        assertEquals(Constants.CTYPE_JSON, reFetched.getContentType());

        b.delete("k").execute();

        // give it time...
        Thread.sleep(500);

        fetched = b.fetch("k").execute();
        assertNull(fetched);
    }

    @Test public void storeWithNullKey() throws Exception {
        Bucket b = client.fetchBucket(bucketName).execute();
        IRiakObject o = b.store(null, "value").withoutFetch().returnBody(true).execute();
        
        String k = o.getKey();
        assertNotNull(k);
        
    }
    
    public static class MyPojo {
        @RiakKey public String key;
        public String value = "Some Value";
    }
    
    @Test public void storePojoWithNullKey() throws Exception {
        Bucket b = client.fetchBucket(bucketName).execute();
        MyPojo p = b.store(new MyPojo()).withoutFetch().returnBody(true).execute();
        String k = p.key;
        assertNotNull(k);
    }
    
    @Ignore("non-deterministic")
    @Test public void byDefaultSiblingsThrowUnresolvedExceptionOnStore() throws Exception {
        final Bucket b = client.createBucket(bucketName).allowSiblings(true).execute();
        b.store("k", "v").execute();

        final int numThreads = 2;
        final Collection<Callable<Boolean>> storers = new ArrayList<Callable<Boolean>>(numThreads);

        final ExecutorService es = Executors.newFixedThreadPool(numThreads);

        for (int i = 0; i < numThreads; i++) {
            final IRiakClient c = getClient();
            c.generateAndSetClientId();
            final Bucket bucket = c.fetchBucket(bucketName).execute();

            storers.add(new Callable<Boolean>() {
                public Boolean call() throws RiakException {
                    try {
                        for (int i = 0; i < 5; i++) {
                            bucket.store("k", Thread.currentThread().getName() + "v" + i).execute();
                            Thread.sleep(50);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return true;
                }
            });
        }

        Collection<Future<Boolean>> results = es.invokeAll(storers);

        for (Future<Boolean> f : results) {
            try {
                f.get();
                fail("Expected siblings");
            } catch (ExecutionException e) {
                assertEquals(UnresolvedConflictException.class, e.getCause().getClass());
            }
        }

        client.updateBucket(b).allowSiblings(false).execute();
    }

    // List Keys
    @Test public void listKeys() throws Exception {
        final Set<String> keys = new LinkedHashSet<String>();

        Bucket b = client.fetchBucket(bucketName + "_kl").execute();

        for (int i = 65; i <= 90; i++) {
            String key = Character.toString((char) i);
            b.store(key, i).execute();
            keys.add(key);
        }

        for (String key : b.keys()) {
            assertTrue(keys.remove(key));
        }

        assertTrue(keys.isEmpty());

        for (String key : b.keys()) {
            b.delete(key).execute();
        }
    }

    // fetch index
    @Test public void fetchIndex() throws Exception {
        Assume.assumeTrue(RiakTestProperties.is2iEnabled());

        final Bucket b = client.fetchBucket(bucketName).execute();

        // create objects with indexes
        IRiakObject o1 = RiakObjectBuilder.newBuilder(bucketName, "k1").withValue("some data")
        .addIndex("twitter", "alfonso").addIndex("age", 37).build();

        IRiakObject o2 = RiakObjectBuilder.newBuilder(bucketName, "k2").withValue("some data")
        .addIndex("twitter", "peter").addIndex("age", 29).build();

        IRiakObject o3 = RiakObjectBuilder.newBuilder(bucketName, "k3").withValue("some data")
        .addIndex("twitter", "zachary").addIndex("age", 30).build();

        IRiakObject o4 = RiakObjectBuilder.newBuilder(bucketName, "k4").withValue("some data")
        .addIndex("another_index", "bob").addIndex("age", 45).build();
		
        // store them
        b.store(o1).execute();
        b.store(o2).execute();
        b.store(o3).execute();
        b.store(o4).execute();

        // retrieve and check indexes are present
        IRiakObject o1r = b.fetch("k1").execute();
        IRiakObject o2r = b.fetch("k2").execute();
        IRiakObject o3r = b.fetch("k3").execute();
        IRiakObject o4r = b.fetch("k4").execute();
		
        assertTrue(o1r.getBinIndex("twitter").contains("alfonso"));
        assertTrue(o1r.getIntIndex("age").contains(37));

        assertTrue(o2r.getBinIndex("twitter").contains("peter"));
        assertTrue(o2r.getIntIndex("age").contains(29));

        assertTrue(o3r.getBinIndex("twitter").contains("zachary"));
        assertTrue(o3r.getIntIndex("age").contains(30));

        assertTrue(o4r.getBinIndex("another_index").contains("bob"));
        assertTrue(o4r.getIntIndex("age").contains(45));

        // fetch by index
        List<String> keys = b.fetchIndex(BinIndex.named("twitter")).withValue("alfonso").execute();

        assertEquals(1, keys.size());
        assertTrue(keys.contains("k1"));

        // fetch int range
        List<String> ageRange = b.fetchIndex(IntIndex.named("age")).from(30).to(40).execute();

        assertEquals(2, ageRange.size());
        assertTrue(ageRange.contains("k1"));
        assertTrue(ageRange.contains("k3"));

        // fetch bin range
        List<String> twitterRange = b.fetchIndex(BinIndex.named("twitter")).from("albert").to("zebediah").execute();

        assertEquals(3, twitterRange.size());
        assertTrue(twitterRange.contains("k1"));
        assertTrue(twitterRange.contains("k2"));
        assertTrue(twitterRange.contains("k3"));

        // fetch no value results
        List<String> empty = b.fetchIndex(BinIndex.named("twitter")).withValue("purdy").execute();

        assertEquals(0, empty.size());

        // fetch no value result
        empty = b.fetchIndex(BinIndex.named("unknown")).withValue("unkown").execute();

        assertEquals(0, empty.size());

        // Without this sleep() the following tests may fail due to previous 
        // keys in riak not yet actually deleted from the call to emptyBucket()
        Thread.sleep(3000L);

        // fetch all keys using magic keys index
        // This is 4 because it picks up all the objects including k4
        List<String> all = b.fetchIndex(KeyIndex.index).from("a").to("z").execute();
        assertEquals(4, all.size());

        // fetch all keys using magic bucket index
        all = b.fetchIndex(BucketIndex.index).withValue("_").execute();
        assertEquals(4, all.size());

    }

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
                            assertArrayEquals(new byte[0], o.getValue());

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
            
        assertTrue(sawDeletedVClock); 
        
    }

    /**
     * test the case where object won't be stored if there is an entry already
     */
    @Test public void conditionalStore_noneMatch() throws Exception {
        final String k = "k";
        final String v1 = "v1";
        final String v2 = "v2";

        final IRiakClient client = getClient();
        final Bucket b = client.fetchBucket(bucketName).execute();

        b.store(k, v1).execute();
        IRiakObject o = b.fetch(k).execute();

        assertEquals(v1, o.getValueAsString());

        try {
            b.store(k, v2).ifNoneMatch(true).execute();
            fail("expected match_found");
        } catch (MatchFoundException e) {
            // NO-OP, all good
        }
    }

    /**
     * test the case where object won't be stored if it has been modified in
     * since (date(HTTP)/vclock(PB)), but it *hasn't* been modified.
     */
    @Test public void conditionalStore_notModified() throws Exception {
        final String k = "k";
        final String v1 = "v1";
        final String v2 = "v2";

        final IRiakClient client = getClient();
        final Bucket b = client.fetchBucket(bucketName).execute();

        b.store(k, v1).execute();
        IRiakObject o = b.fetch(k).execute();

        assertEquals(v1, o.getValueAsString());

        b.store(k, v2).ifNotModified(true).execute();

        IRiakObject o2 = b.fetch(k).execute();

        assertEquals(v2, o2.getValueAsString());
    }

    /**
     * test the case where object won't be stored if it has been modified in
     * since (date(HTTP)/vclock(PB)), uses a custom mutator, threads and a mutex
     * to force an update to occur inbetween when a pre-store fetch has executed
     * and when the actual store is executed.
     */
    @Test public void conditionalStore_modified() throws Exception {
        final String k = "k";
        final String v1 = "v1";
        final String v2 = "v2";
        final String v3 = "v3";
        final SynchronousQueue<Integer> sync = new SynchronousQueue<Integer>(true);
        final CountDownLatch endLatch = new CountDownLatch(1);

        final IRiakClient client = getClient();
        final Bucket b = client.fetchBucket(bucketName).execute();
        b.store(k, v1).execute();

        final Runnable helper = new Runnable() {
            public void run() {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        sync.take(); // block until the other thread has fetched
                        Thread.sleep(1000); // throw in a sleep for the HTTP API
                        b.store(k, v3).execute();
                        sync.put(1); // tell the other thread that we've updated
                                     // the value
                    }
                } catch (RiakException e) {
                    // fatal
                    fail("exception in helper, storing value");
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    // used to cause thread to exit
                    Thread.currentThread().interrupt();
                }
            }
        };

        final Thread helperThread = new Thread(helper);
        helperThread.start();
        final Runnable failer = new Runnable() {
            public void run() {
                try {
                    b.store(k, v2).ifNotModified(true).withMutator(new Mutation<IRiakObject>() {

                        public IRiakObject apply(IRiakObject original) {
                            try {
                                sync.put(1); // tell the other thread to store
                                sync.take(); // block until it has stored
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new RuntimeException(e);
                            }
                            return original;
                        }
                    }).execute();
                    fail("Expected an exception");
                } catch (RiakRetryFailedException e) {
                    assertTrue((e.getCause() instanceof ModifiedException));
                    helperThread.interrupt();
                    endLatch.countDown();
                }
            }
        };

        final Thread failerThread = new Thread(failer);
        failerThread.start();

        boolean success = endLatch.await(10, TimeUnit.SECONDS);

        assertTrue(success);
    }

    @Test public void deleteWithFetchedVClock() throws Exception {
        Bucket b = client.fetchBucket(bucketName).execute();
        b.store("k", "v").execute();

        IRiakObject fetched = b.fetch("k").execute();
        assertEquals("v", fetched.getValueAsString());

        b.delete("k").r(1).pr(1).w(1).dw(1).pw(1).fetchBeforeDelete(true).execute();

        Thread.sleep(200);

        fetched = b.fetch("k").execute();

        assertNull(fetched);
    }
 }
