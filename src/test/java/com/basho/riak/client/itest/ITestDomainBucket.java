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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.bucket.DomainBucket;
import com.basho.riak.client.cap.DefaultRetrier;
import com.basho.riak.client.cap.Mutation;
import com.basho.riak.client.cap.MutationProducer;
import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.raw.DeleteMeta;
import com.basho.riak.client.raw.FetchMeta;
import com.basho.riak.client.raw.StoreMeta;
import com.megacorp.commerce.CartMerger;
import com.megacorp.commerce.MergeCartResolver;
import com.megacorp.commerce.ShoppingCart;
import java.lang.reflect.Field;

/**
 * A DomainBucket is a wrapper around a bucket that uses a preset conflict
 * resolver, [ mutator, converter, r, rw, dw, w, retries etc]
 * 
 * @author russell
 * 
 */
public abstract class ITestDomainBucket {

    protected IRiakClient client;
    protected String bucketName;

    @Before public void setUp() throws RiakException {
        this.client = getClient();
        this.bucketName = this.getClass().getName();
        emptyBucket(bucketName, client);
    }

    public abstract IRiakClient getClient() throws RiakException;

    @Test public void useDomainBucket() throws Exception {
        final String userId = UUID.randomUUID().toString();

        //get two clients with different IDs
        client.generateAndSetClientId();
        // create the bucket we're to use, allow siblings
        client.createBucket(bucketName).allowSiblings(true).nVal(3).execute();

        IRiakClient client2 = getClient();
        client2.generateAndSetClientId();

        // get the domain buckets, 2 buckets with different client ids simulates
        // two web server nodes or some such scenario
        final DomainBucket<ShoppingCart> carts = getDomainBucket(client, bucketName);
        final DomainBucket<ShoppingCart> carts2 = getDomainBucket(client2, bucketName);

        // create the initial cart
        final ShoppingCart cart = new ShoppingCart(userId);

        cart.addItem("coffee");
        cart.addItem("fixie");
        cart.addItem("moleskine");

        // store it
        final ShoppingCart storedCart = carts.store(cart);

        assertNotNull(storedCart);
        assertEquals(cart.getUserId(), storedCart.getUserId());
        assertEquals(cart, storedCart);

        // Fetch it and add things to it
        final ShoppingCart cart1 = carts.fetch(userId);
        final ShoppingCart cart2 = carts2.fetch(userId);

        assertEquals(cart1, cart2);

        cart1.addItem("bowtie");
        cart1.addItem("nail gun");

        cart2.addItem("hippo");
        cart2.addItem("jasmin tea");

        assertFalse(cart1.equals(cart2));

        carts.store(cart1);
        carts2.store(cart2);

        final String[] expectedMergedCart = { "coffee", "fixie", "moleskine", "hippo", "jasmin tea", "nail gun",
                                             "bowtie" };

        // this should contain the merged items
        final ShoppingCart finalCart = carts.fetch(userId);

        for (String item : expectedMergedCart) {
            assertTrue("Expected cart to contain: " + item, finalCart.hasItem(item));
        }

        client.createBucket(bucketName).allowSiblings(false).nVal(3).execute();
    }

    /**
     * Generate a domain bucket.
     * 
     * @param client
     * @param bucketName
     * @return
     * @throws RiakRetryFailedException
     */
    private DomainBucket<ShoppingCart> getDomainBucket(final IRiakClient client, String bucketName) throws RiakRetryFailedException {
        final Bucket b = client.fetchBucket(bucketName).execute();

        return DomainBucket.builder(b, ShoppingCart.class)
            .mutationProducer(new MutationProducer<ShoppingCart>() {
                public Mutation<ShoppingCart> produce(ShoppingCart o) {
                    return new CartMerger(o);
                }
            })
            .withResolver(new MergeCartResolver())
            .returnBody(true)
            .retrier(DefaultRetrier.attempts(3))
            .w(1)
            .dw(1)
            .r(1)
            .rw(1)
            .build();
    }

    @Test public void minimalDomainBucketWorks() throws Exception {
        final String userId = UUID.randomUUID().toString();
        Bucket b = client.fetchBucket(bucketName).execute();
        DomainBucket<ShoppingCart> carts = DomainBucket.builder(b,
                ShoppingCart.class).build();

        ShoppingCart cart = new ShoppingCart(userId);
        cart.addItem("elephant");
        carts.store(cart);

        ShoppingCart cart2 = carts.fetch(userId);

        assertNotNull(cart2);
        assertEquals(userId, cart2.getUserId());
        assertTrue(cart2.hasItem("elephant"));

        carts.delete(cart2);

        Thread.sleep(500);

        ShoppingCart cart3 = carts.fetch(userId);

        assertNull(cart3);
    }
    
    @Test public void domainBucketAcceptsQuora() throws Exception {
        Bucket b = client.fetchBucket(bucketName).execute();
        DomainBucket<ShoppingCart> carts = DomainBucket.builder(b,
                ShoppingCart.class)
                .dw(Quora.ONE)
                .pw(Quora.ONE)
                .w(Quora.ALL)
                .pr(Quora.QUORUM)
                .r(Quora.ONE)
                .rw(Quora.ONE).build();
        
        // Using reflection to get at the underlying private Meta objects 
        Class domainBucketClass = carts.getClass();
        Field storeMetaField = domainBucketClass.getDeclaredField("storeMeta");
        storeMetaField.setAccessible(true);
        StoreMeta storeMeta = (StoreMeta)storeMetaField.get(carts);
        
        assertTrue(storeMeta.hasDw() && (storeMeta.getDw().equals(new Quorum(Quora.ONE))));
        assertTrue(storeMeta.hasPw() && (storeMeta.getPw().equals(new Quorum(Quora.ONE))));
        assertTrue(storeMeta.hasW() && (storeMeta.getW().equals(new Quorum(Quora.ALL))));
        
        Field fetchMetaField = domainBucketClass.getDeclaredField("fetchMeta");
        fetchMetaField.setAccessible(true);
        FetchMeta fetchMeta = (FetchMeta)fetchMetaField.get(carts);
        
        assertTrue(fetchMeta.hasR() && (fetchMeta.getR().equals(new Quorum(Quora.ONE))));
        assertTrue(fetchMeta.hasPr() && (fetchMeta.getPr().equals(new Quorum(Quora.QUORUM))));
        
        Field deleteMetaField = domainBucketClass.getDeclaredField("deleteMeta");
        deleteMetaField.setAccessible(true);
        DeleteMeta deleteMeta = (DeleteMeta)deleteMetaField.get(carts);
        
        assertTrue(deleteMeta.hasRw() && (deleteMeta.getRw().equals(new Quorum(Quora.ONE))));
        
    }
    
}
