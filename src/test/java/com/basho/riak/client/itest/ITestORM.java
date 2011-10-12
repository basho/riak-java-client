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
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assume;
import org.junit.Test;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.TestProperties;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.DefaultRetrier;
import com.basho.riak.client.convert.NoKeySpecifedException;
import com.basho.riak.client.query.indexes.BinIndex;
import com.basho.riak.client.query.indexes.IntIndex;
import com.megacorp.commerce.Customer;
import com.megacorp.commerce.Department;
import com.megacorp.commerce.LegacyCart;
import com.megacorp.commerce.ShoppingCart;

/**
 * @author russell
 * 
 */
public abstract class ITestORM extends ITestBucket {

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

        carts.store(cart).returnBody(false).withRetrier(DefaultRetrier.attempts(2)).execute();

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
            carts.store(cart).returnBody(false).withRetrier(new DefaultRetrier(3)).execute();
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

    @Test public void storeDomainObjectWithUserMeta() throws Exception {
        final String bucketName = UUID.randomUUID().toString() + "_users";
        final String userId = UUID.randomUUID().toString();
        final String email = "customer@megacorp.com";
        final String languageCode = "en";
        final String favColor = "fav-color";
        final String blue = "blue";
        final String username = "userX";

        final Bucket users = client.createBucket(bucketName).execute();

        final Customer c1 = new Customer(userId);

        c1.setUsername(username);
        c1.setEmailAddress(email);
        c1.setLanguageCode(languageCode);
        c1.addPreference(favColor, blue);

        users.store(c1).execute();

        // fetch it as an IRiakObject and check the meta data
        IRiakObject iro = users.fetch(userId).execute();
        Map<String, String> meta = iro.getMeta();
        // check that meta values haven't leaked into the JSON
        JsonNode value = new ObjectMapper().readTree(iro.getValueAsString());

        assertEquals(3, value.size());
        assertEquals(username, value.get("username").getTextValue());
        assertEquals(email, value.get("emailAddress").getTextValue());
        assertEquals(null, value.get("shoeSize").getTextValue());
        assertEquals(2, meta.size());
        assertEquals(languageCode, meta.get("language-pref"));
        assertEquals(blue, meta.get(favColor));

        // fetch it as a Customer and check the meta data
        Customer actual = users.fetch(userId, Customer.class).execute();
        Map<String, String> prefs = actual.getPreferences();

        assertEquals(languageCode, actual.getLanguageCode());
        assertEquals(1, prefs.size());
        assertEquals(blue, prefs.get(favColor));
    }

    @Test public void storeDomainObjectWithIndexes() throws Exception {
        Assume.assumeTrue(TestProperties.is2iEnabled());
        final String bucketName = UUID.randomUUID().toString() + "_users";
        final String userId = UUID.randomUUID().toString();
        final String email = "customer@megacorp.com";
        final String username = "userX";
        final int shoeSize = 43;

        final Bucket users = client.createBucket(bucketName).execute();

        final Customer c1 = new Customer(userId);

        c1.setUsername(username);
        c1.setEmailAddress(email);
        c1.setShoeSize(shoeSize);

        users.store(c1).execute();

        // retrieve as riak object and check indexes are present
        IRiakObject iro = users.fetch(userId).execute();

        Map<IntIndex, Set<Integer>> intIndexes = iro.allIntIndexes();

        assertEquals(1, intIndexes.size());
        Set<Integer> si = intIndexes.get(IntIndex.named("shoe-size"));
        assertEquals(1, si.size());
        assertEquals(shoeSize, si.iterator().next().intValue());

        Map<BinIndex, Set<String>> binIndexes = iro.allBinIndexes();

        assertEquals(1, binIndexes.size());
        Set<String> sb = binIndexes.get(BinIndex.named("email"));
        assertEquals(1, sb.size());
        assertEquals(email, sb.iterator().next());

        // retrieve as a domain object and check indexes
        Customer actual = users.fetch(userId, Customer.class).execute();

        assertEquals(email, actual.getEmailAddress());
        assertEquals(shoeSize, actual.getShoeSize());

        // do index queries
        List<String> keys = users.fetchIndex(BinIndex.named("email")).withValue(email).execute();

        assertEquals(1, keys.size());
        assertEquals(userId, keys.get(0));

        keys = users.fetchIndex(IntIndex.named("shoe-size")).from(0).to(100).execute();
        assertEquals(1, keys.size());
        assertEquals(userId, keys.get(0));
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

    @Test public void storeDomainObjectWithLinks() throws Exception {
        final String bucketName = UUID.randomUUID().toString() + "_users";
        final String deptId = UUID.randomUUID().toString();
        final String name = "Irwin Firwin";

        final Bucket depts = client.createBucket(bucketName).execute();
        Department dept = new Department(deptId);
        dept.setName(name);

        Collection<RiakLink> employees = new ArrayList<RiakLink>();
        employees.add(new RiakLink("employees", "123", "manager"));
        employees.add(new RiakLink("employees", "124", "staff"));
        employees.add(new RiakLink("employees", "125", "staff"));

        dept.setEmployees(employees);

        depts.store(dept).execute();

        // fetch as IRiakObject and check links
        IRiakObject o = depts.fetch(deptId).execute();
        List<RiakLink> links = o.getLinks();

        assertEquals(employees, links);

        // check links are *not* in the model
        JsonNode data = new ObjectMapper().readTree(o.getValueAsString());
        assertNull(data.get("employees"));

        // fetch as dept, and check links
        Department actual = depts.fetch(deptId, Department.class).execute();

        assertEquals(employees, actual.getEmployees());
    }
}
