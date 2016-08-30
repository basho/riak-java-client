/*
 * Copyright 2013 Basho Technologies Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.core.operations.itest;

import com.basho.riak.client.api.commands.datatypes.*;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.DtFetchOperation;
import com.basho.riak.client.core.operations.DtUpdateOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.crdt.types.*;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Assume;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class ITestCrdtApi extends ITestAutoCleanupBase
{
    @Test
    public void simpleTest() throws ExecutionException, InterruptedException
    {
        Assume.assumeTrue(testCrdt);
        /**
         * Update some info about a user in a table of users
         */

        resetAndEmptyBucket(new Namespace(mapBucketType, bucketName));

        // BinaryValues make it look messy, so define them all here.
        final String numLogins = "logins";
        final String lastLoginTime = "last-login";
        final ByteBuffer nowBinary = ByteBuffer.allocate(8).putLong(System.currentTimeMillis());
        final byte[] now = nowBinary.array();
        final String username = "username";
        final String loggedIn = "logged-in";
        final String shoppingCart = "cart";

        BinaryValue key = BinaryValue.create("user-info");

        /*

            Data structure:

            bucket-type == asMap
            -> "username" : asMap
              -> "logins"     : counter
              -> "last-login" : register
              -> "logged-in"  : flag
              -> "cart"       : asSet

         */

        // Build a shopping cart. We're buying the digits!!!!
        ByteBuffer buffer = ByteBuffer.allocate(4);
        SetUpdate favorites = new SetUpdate();
        for (int i = 0; i < 10; ++i)
        {
            buffer.putInt(i);
            favorites.add(BinaryValue.create(buffer.array()));
            buffer.rewind();
        }

        // Create an update for the user's values
        MapUpdate userMapUpdate = new MapUpdate()
            .update(numLogins, new CounterUpdate(1))                // counter
            .update(lastLoginTime, new RegisterUpdate(now))     // register
            .update(loggedIn, new FlagUpdate(true))                   // flag
            .update(shoppingCart, favorites);              // asSet

        // Now create an update for the user's entry
        MapUpdate userEntryUpdate = new MapUpdate()
            .update(username, userMapUpdate);

        Location location = new Location(new Namespace(mapBucketType, bucketName), key);
        DtUpdateOperation update =
            new DtUpdateOperation.Builder(location)
                .withOp(userEntryUpdate.getOp())
                .build();

        cluster.execute(update);
        update.get();

        DtFetchOperation fetch = new DtFetchOperation.Builder(location)
            .build();
        cluster.execute(fetch);

        // users
        RiakDatatype element = fetch.get().getCrdtElement();
        assertNotNull(element);
        assertTrue(element.isMap());
        RiakMap usersMap = element.getAsMap();

        // username
        List<RiakDatatype> usernameElement = usersMap.get(BinaryValue.create(username));
        assertNotNull(usernameElement);
        assertEquals(1, usernameElement.size());
        assertTrue(usernameElement.get(0).isMap());
        RiakMap usernameMap = usernameElement.get(0).getAsMap();

        // logins - counter
        List<RiakDatatype> numLoginsElement = usernameMap.get(BinaryValue.create(numLogins));
        assertNotNull(numLoginsElement);
        assertEquals(1, numLoginsElement.size());
        assertTrue(numLoginsElement.get(0).isCounter());
        RiakCounter numLoginsCounter = numLoginsElement.get(0).getAsCounter();
        assertEquals((Long) 1L, numLoginsCounter.view());

        // last-login - register
        List<RiakDatatype> lastLoginTimeElement = usernameMap.get(BinaryValue.create(lastLoginTime));
        assertNotNull(lastLoginTimeElement);
        assertEquals(1, lastLoginTimeElement.size());
        assertTrue(lastLoginTimeElement.get(0).isRegister());
        RiakRegister lastLoginTimeRegister = lastLoginTimeElement.get(0).getAsRegister();
        assertTrue(Arrays.equals(now, lastLoginTimeRegister.getValue().getValue()));

        // logged-in - flag
        List<RiakDatatype> loggedInElement = usernameMap.get(BinaryValue.create(loggedIn));
        assertNotNull(loggedInElement);
        assertEquals(1, loggedInElement.size());
        assertTrue(loggedInElement.get(0).isFlag());
        RiakFlag loggedInFlag = loggedInElement.get(0).getAsFlag();
        assertEquals(true, loggedInFlag.getEnabled());

        // cart - asSet
        List<RiakDatatype> shoppingCartElement = usernameMap.get(BinaryValue.create(shoppingCart));
        assertNotNull(shoppingCartElement);
        assertEquals(1, shoppingCartElement.size());
        assertTrue(shoppingCartElement.get(0).isSet());
    }

    @Test
    public void testNonExistingDatatypeReturnsBottomValueAndNotFoundFlag() throws ExecutionException, InterruptedException
    {
        RiakMap mapBottomValue = new RiakMap(Collections.<RiakMap.MapEntry>emptyList());
        RiakCounter counterBottomValue = new RiakCounter(0);
        RiakSet setBottomValue = new RiakSet(Collections.<BinaryValue>emptyList());

        // Maps
        Location mapLocation = new Location(new Namespace(mapBucketType, bucketName), "404");
        DtFetchOperation mapFetch = new DtFetchOperation.Builder(mapLocation).build();
        final RiakFuture<DtFetchOperation.Response, Location> mapFuture = cluster.execute(mapFetch);

        final DtFetchOperation.Response mapResponse = mapFuture.get();
        assertTrue(mapResponse.isNotFound());
        assertTrue(mapResponse.hasCrdtElement());
        assertEquals(mapBottomValue, mapResponse.getCrdtElement().getAsMap());

        // Sets
        Location setLocation = new Location(new Namespace(setBucketType, bucketName), "404");
        DtFetchOperation setFetch = new DtFetchOperation.Builder(setLocation).build();
        final RiakFuture<DtFetchOperation.Response, Location> setFuture = cluster.execute(setFetch);

        final DtFetchOperation.Response setResponse = setFuture.get();
        assertTrue(setResponse.isNotFound());
        assertTrue(setResponse.hasCrdtElement());
        assertEquals(setBottomValue, setResponse.getCrdtElement().getAsSet());

        // Counters
        Location counterLocation = new Location(new Namespace(counterBucketType, bucketName), "404");
        DtFetchOperation counterFetch = new DtFetchOperation.Builder(counterLocation).build();
        final RiakFuture<DtFetchOperation.Response, Location> counterFuture = cluster.execute(counterFetch);

        final DtFetchOperation.Response counterResponse = counterFuture.get();
        assertTrue(counterResponse.isNotFound());
        assertTrue(counterResponse.hasCrdtElement());
        assertEquals(counterBottomValue, counterResponse.getCrdtElement().getAsCounter());
    }
}
