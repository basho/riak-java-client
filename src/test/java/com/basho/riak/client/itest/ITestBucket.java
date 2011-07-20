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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.DefaultRetrier;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.NoKeySpecifedException;
import com.basho.riak.client.convert.PassThroughConverter;
import com.megacorp.commerce.LegacyCart;
import com.megacorp.commerce.ShoppingCart;

/**
 * @author russell
 * 
 */
public abstract class ITestBucket {

    protected IRiakClient client;

    @Before public void setUp() throws RiakException {
        client = getClient();
    }

    protected abstract IRiakClient getClient() throws RiakException;

    @Test public void basicStore() throws Exception {
        final String bucketName = UUID.randomUUID().toString();

        Bucket b = client.fetchBucket(bucketName).execute();
        IRiakObject o = b.store("k", "v").execute();
        assertNull(o);

        IRiakObject fetched = b.fetch("k").execute();
        assertEquals("v", fetched.getValueAsString());

        // now update that riak object
        b.store("k", "my new value").execute();
        fetched = b.fetch("k").execute();
        assertEquals("my new value", fetched.getValueAsString());

        // add links and user meta
        final RiakLink link1 = new RiakLink("b", "k2", "brother");
        final RiakLink link2 = new RiakLink("b", "k3", "sister");
        fetched.addLink(link1).addLink(link2);
        fetched.addUsermeta("meta1", "metaValue1").addUsermeta("meta2", "metaValue2");

        b.store(fetched).withConverter(new PassThroughConverter()).execute();

        IRiakObject reFetched = b.fetch("k").execute();

        assertEquals(2, reFetched.numLinks());
        assertTrue(reFetched.hasLink(link1));
        assertTrue(reFetched.hasLink(link2));

        assertEquals("metaValue1", reFetched.getUsermeta("meta1"));
        assertEquals("metaValue2", reFetched.getUsermeta("meta2"));

        b.delete("k").execute();

        // give it time...
        Thread.sleep(500);

        fetched = b.fetch("k").execute();
        assertNull(fetched);
    }

    @Test public void byDefaultSiblingsThrowUnresolvedExceptionOnStore() throws Exception {
        final String bucketName = UUID.randomUUID().toString();

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

        // TODO clean up your mess (teardown)
    }

    /**
     * @see ITestDomainBucket
     * @throws Exception
     */
    @Test public void storeDomainObjectWithKeyAnnotation() throws Exception {
        final String bucketName = UUID.randomUUID().toString() + "_carts";
        final String userId = UUID.randomUUID().toString();

        final Bucket carts = client.createBucket(bucketName).allowSiblings(true).execute();

        final ShoppingCart cart = new ShoppingCart(userId);

        cart.addItem("coffee");
        cart.addItem("fixie");
        cart.addItem("moleskine");

        carts.store(cart).returnBody(false).retrier(DefaultRetrier.attempts(2)).execute();

        final ShoppingCart fetchedCart = carts.fetch(cart).execute();

        assertNotNull(fetchedCart);
        assertEquals(cart.getUserId(), fetchedCart.getUserId());
        assertEquals(cart, fetchedCart);

        carts.delete(fetchedCart).rw(3).execute();

        Thread.sleep(500);

        assertNull(carts.fetch(userId).execute());
    }

    @Test public void storeDomainObjectWithoutKeyAnnotation() throws Exception {
        final String bucketName = UUID.randomUUID().toString() + "_carts";
        final String userId = UUID.randomUUID().toString();

        final Bucket carts = client.createBucket(bucketName).allowSiblings(true).execute();

        final LegacyCart cart = new LegacyCart();
        cart.setUserId(userId);

        cart.addItem("coffee");
        cart.addItem("fixie");
        cart.addItem("moleskine");

        try {
            carts.store(cart).returnBody(false).retrier(new DefaultRetrier(3)).execute();
            fail("Expected NoKeySpecifiedException");
        } catch (NoKeySpecifedException e) {
            // NO-OP
        }

        carts.store(userId, cart).returnBody(false).execute();

        try {
            carts.fetch(cart).execute();
            fail("Expected NoKeySpecifiedException");
        } catch (NoKeySpecifedException e) {
            // NO-OP
        }

        final LegacyCart fetchedCart = carts.fetch(userId, LegacyCart.class).execute();

        assertNotNull(fetchedCart);
        assertEquals(cart.getUserId(), fetchedCart.getUserId());
        assertEquals(cart, fetchedCart);

        try {
            carts.delete(cart).execute();
            fail("Expected NoKeySpecifiedException");
        } catch (NoKeySpecifedException e) {
            // NO-OP
        }

        carts.delete(userId).rw(3).execute();

        Thread.sleep(500);

        assertNull(carts.fetch(userId).execute());
    }

    @Test public void storeMap() throws Exception {
        final String bucketName = UUID.randomUUID().toString() + "_maps";
        final String key = UUID.randomUUID().toString();

        final Bucket maps = client.createBucket(bucketName).allowSiblings(true).execute();
        
        final Map<String, String> myMap = new HashMap<String, String>();
        myMap.put("size", "s");
        myMap.put("colour", "red");
        myMap.put("style", "short-sleeve");
        
        maps.store(key, myMap).returnBody(false).w(2).execute();
        
        @SuppressWarnings("unchecked") final Map<String, String> fetchedMap = maps.fetch(key, Map.class).execute();
        
        assertEquals(myMap, fetchedMap);
    }
    
    @Test public void storeList() throws Exception {
        final String bucketName = UUID.randomUUID().toString() + "_lists";
        final String key = UUID.randomUUID().toString();

        final Bucket lists = client.createBucket(bucketName).allowSiblings(true).execute();
        
        final Collection<String> myList = new ArrayList<String>();
        myList.add("red");
        myList.add("yellow");
        myList.add("pink");
        myList.add("green");
        
        lists.store(key, myList).returnBody(false).w(2).execute();
        
        @SuppressWarnings("unchecked") final Collection<String> fetchedList = lists.fetch(key, Collection.class).execute();
        
        assertEquals(myList, fetchedList);
    }
    
    // List Keys
    @Test public void listKeys() throws Exception {
        final Set<String> keys = new LinkedHashSet<String>();

        final String bucketName = UUID.randomUUID().toString();

        Bucket b = client.fetchBucket(bucketName).execute();

        for (int i = 65; i <= 90; i++) {
            String key = Character.toString((char) i);
            b.store(key, i).execute();
            keys.add(key);
        }

        for (String key : b.keys()) {
            assertTrue(keys.remove(key));
        }

        assertTrue(keys.isEmpty());
    }
}
