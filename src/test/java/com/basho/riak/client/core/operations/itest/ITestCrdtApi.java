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

import com.basho.riak.client.core.operations.DtFetchOperation;
import com.basho.riak.client.core.operations.DtUpdateOperation;
import static com.basho.riak.client.core.operations.itest.ITestBase.bucketName;
import com.basho.riak.client.operations.datatypes.*;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.crdt.types.*;
import com.basho.riak.client.util.BinaryValue;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import static junit.framework.Assert.*;
import org.junit.Assume;

public class ITestCrdtApi extends ITestBase
{

    @Test
    public void simpleTest() throws ExecutionException, InterruptedException
    {
        Assume.assumeTrue(testCrdt);
        /**
         * Update some info about a user in a table of users
         */

        resetAndEmptyBucket(new Location(bucketName).setBucketType(mapBucketType));

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
            favorites.add(buffer.array());
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

        Location location = new Location(bucketName).setBucketType(mapBucketType).setKey(key);
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
        CrdtElement element = fetch.get().getCrdtElement();
        assertNotNull(element);
        assertTrue(element.isMap());
        CrdtMap usersMap = element.getAsMap();

        // username
        CrdtElement usernameElement = usersMap.get(BinaryValue.create(username));
        assertNotNull(usernameElement);
        assertTrue(usernameElement.isMap());
        CrdtMap usernameMap = usernameElement.getAsMap();

        // logins - counter
        CrdtElement numLoginsElement = usernameMap.get(BinaryValue.create(numLogins));
        assertNotNull(numLoginsElement);
        assertTrue(numLoginsElement.isCounter());
        CrdtCounter numLoginsCounter = numLoginsElement.getAsCounter();
        assertEquals(1, numLoginsCounter.getValue());

        // last-login - register
        CrdtElement lastLoginTimeElement = usernameMap.get(BinaryValue.create(lastLoginTime));
        assertNotNull(lastLoginTimeElement);
        assertTrue(lastLoginTimeElement.isRegister());
        CrdtRegister lastLoginTimeRegister = lastLoginTimeElement.getAsRegister();
        assertTrue(Arrays.equals(now, lastLoginTimeRegister.getValue().getValue()));

        // logged-in - flag
        CrdtElement loggedInElement = usernameMap.get(BinaryValue.create(loggedIn));
        assertNotNull(loggedInElement);
        assertTrue(loggedInElement.isFlag());
        CrdtFlag loggedInFlag = loggedInElement.getAsFlag();
        assertEquals(true, loggedInFlag.getEnabled());

        // cart - asSet
        CrdtElement shoppingCartElement = usernameMap.get(BinaryValue.create(shoppingCart));
        assertNotNull(shoppingCartElement);
        assertTrue(shoppingCartElement.isSet());

    }

}
