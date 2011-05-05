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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.bucket.DomainBucket;
import com.basho.riak.client.cap.DefaultRetrier;
import com.megacorp.commerce.MergeCartResolver;
import com.megacorp.commerce.ShoppingCart;

/**
 * A DomainBucket is a wrapper around a bucket that uses a preset conflict
 * resolver, [ mutator, converter, r, rw, dw, w, retries etc]
 * 
 * @author russell
 * 
 */
public abstract class ITestDomainBucket {

    protected IRiakClient client;

    @Before public void setUp() throws RiakException {
        this.client = getClient();
    }

    public abstract IRiakClient getClient() throws RiakException;

    @Test public void useDomainBucket() throws Exception {
        final String bucketName = UUID.randomUUID().toString() + "_carts";
        final String userId = UUID.randomUUID().toString();

        client.generateAndSetClientId();

        final Bucket b = client.createBucket(bucketName).allowSiblings(true).nVal(3).execute();

        final DomainBucket<ShoppingCart> carts = DomainBucket.builder(b, ShoppingCart.class)
            .withResolver(new MergeCartResolver())
            .returnBody(true)
            .retrier(DefaultRetrier.attempts(3))
            .w(1)
            .dw(1)
            .r(1)
            .rw(1)
            .build();

        final ShoppingCart cart = new ShoppingCart(userId);

        cart.addItem("coffee");
        cart.addItem("fixie");
        cart.addItem("moleskine");

        final ShoppingCart storedCart = carts.store(cart);

        assertNotNull(storedCart);
        assertEquals(cart.getUserId(), storedCart.getUserId());
        assertEquals(cart, storedCart);

        final ExecutorService es = Executors.newFixedThreadPool(2);
        final Collection<Callable<ShoppingCart>> tasks = new ArrayList<Callable<ShoppingCart>>();

        tasks.add(new Callable<ShoppingCart>() {
            public ShoppingCart call() throws Exception {
                final ShoppingCart cart = carts.fetch(userId);
                cart.addItem("bowtie");
                cart.addItem("nail gun");
                return carts.store(cart);
            }
        });

        tasks.add(new Callable<ShoppingCart>() {
            public ShoppingCart call() throws Exception {
                final ShoppingCart cart = carts.fetch(userId);
                cart.addItem("hippo");
                cart.addItem("jasmin tea");
                return carts.store(cart);
            }
        });

        es.invokeAll(tasks);

        final String[] expectedMergesCart = { "coffee", "fixie", "moleskine", "hippo", "jasmin tea", "nail gun",
                                             "bowtie" };

        final ShoppingCart finalCart = carts.fetch(userId);

        assertTrue(finalCart.hasAll(Arrays.asList(expectedMergesCart)));
    }
}
