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
import com.basho.riak.client.operations.crdt.MapMutation;
import com.basho.riak.client.query.crdt.ops.MapOp;
import com.basho.riak.client.query.crdt.types.*;
import com.basho.riak.client.util.ByteArrayWrapper;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import static com.basho.riak.client.operations.crdt.CounterMutation.increment;
import static com.basho.riak.client.operations.crdt.CrdtMutation.forMap;
import static com.basho.riak.client.operations.crdt.FlagMutation.enabled;
import static com.basho.riak.client.operations.crdt.RegisterMutation.registerValue;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

public class ITestCrdtApi extends ITestBase
{

    @Test
    public void simpleTest() throws ExecutionException, InterruptedException
    {

        /**
         * Update some info about a user in a table of users
         */

        resetAndEmptyBucket(bucketName, mapBucketType);

        // ByteArrayWrappers make it look messy, so define them all here.
        ByteArrayWrapper numLogins = ByteArrayWrapper.create("logins");
        ByteArrayWrapper lastLoginTime = ByteArrayWrapper.create("last-login");
        ByteBuffer nowBinary = ByteBuffer.allocate(8).putLong(System.currentTimeMillis());
        ByteArrayWrapper now = ByteArrayWrapper.create(nowBinary.array());
        ByteArrayWrapper users = ByteArrayWrapper.create("users");
        ByteArrayWrapper username = ByteArrayWrapper.create("username");
        ByteArrayWrapper loggedIn = ByteArrayWrapper.create("logged-in");

        // Create a builder for the user and update the values
        MapMutation userMapUpdate = forMap()
            .update(numLogins, increment())
            .update(lastLoginTime, registerValue(now))
            .update(loggedIn, enabled());

        // Now create an update for the user's entry
        MapMutation userEntryUpdate = forMap().update(username, userMapUpdate);
        MapMutation userInfoUpdate = forMap().update(users, userEntryUpdate);

        ByteArrayWrapper key = ByteArrayWrapper.create("user-info");

        DtUpdateOperation update =
            new DtUpdateOperation(bucketName, mapBucketType)
                .withOp((MapOp) userInfoUpdate.getOp())
                .withKey(key);

        cluster.execute(update);
        update.get();

        DtFetchOperation fetch = new DtFetchOperation(bucketName, key).withBucketType(mapBucketType);
        cluster.execute(fetch);

        // enclosing map
        CrdtElement element = fetch.get();
        assertNotNull(element);
        assertTrue(element.isMap());
        CrdtMap outerMap = element.getAsMap();

        // users
        CrdtElement usersElement = outerMap.get(users);
        assertNotNull(usersElement);
        assertTrue(usersElement.isMap());
        CrdtMap usersMap = usersElement.getAsMap();

        // username
        CrdtElement usernameElement = usersMap.get(username);
        assertNotNull(usernameElement);
        assertTrue(usernameElement.isMap());
        CrdtMap usernameMap = usernameElement.getAsMap();

        // logins
        CrdtElement numLoginsElement = usernameMap.get(numLogins);
        assertNotNull(numLoginsElement);
        assertTrue(numLoginsElement.isCounter());
        CrdtCounter numLoginsCounter = numLoginsElement.getAsCounter();
        assertEquals(1, numLoginsCounter.getValue());

        // last-login
        CrdtElement lastLoginTimeElement = usernameMap.get(lastLoginTime);
        assertNotNull(lastLoginTimeElement);
        assertTrue(lastLoginTimeElement.isRegister());
        CrdtRegister lastLoginTimeRegister = lastLoginTimeElement.getAsRegister();
        assertEquals(now, lastLoginTimeRegister.getValue());

        // logged-in
        CrdtElement loggedInElement = usernameMap.get(loggedIn);
        assertNotNull(loggedInElement);
        assertTrue(loggedInElement.isFlag());
        CrdtFlag loggedInFlag = loggedInElement.getAsFlag();
        assertEquals(true, loggedInFlag.getEnabled());

    }

}
