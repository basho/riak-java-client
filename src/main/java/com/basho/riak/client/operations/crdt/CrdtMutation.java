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
package com.basho.riak.client.operations.crdt;

import com.basho.riak.client.query.crdt.ops.CrdtOp;
import com.basho.riak.client.util.ByteArrayWrapper;

import java.nio.ByteBuffer;

import static com.basho.riak.client.operations.crdt.CounterMutation.increment;
import static com.basho.riak.client.operations.crdt.FlagMutation.enabled;
import static com.basho.riak.client.operations.crdt.RegisterMutation.registerValue;

public abstract class CrdtMutation
{

    public abstract CrdtOp getOp();

    public static MapMutation forMap()
    {
        return new MapMutation();
    }

    public static SetMutation forSet()
    {
        return new SetMutation();
    }

    public static CounterMutation forCounter()
    {
        return new CounterMutation();
    }

    public static void main(String... args)
    {

        /**
         * Update some info about a user in a table of users
         */

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

        CrdtOp op = userInfoUpdate.getOp();

        // TBD, pass userInfoUpdate to the operation to be updated

    }

}
