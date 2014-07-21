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
package com.basho.riak.client.core.operations;

import com.basho.riak.client.core.query.crdt.ops.MapOp;
import com.basho.riak.client.core.query.crdt.ops.RegisterOp;
import com.basho.riak.client.core.query.crdt.ops.SetOp;
import com.basho.riak.client.core.query.crdt.ops.CounterOp;
import com.basho.riak.client.core.query.crdt.ops.FlagOp;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakDtPB;
import com.google.protobuf.ByteString;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class DtUpdateOperationTest
{
    private final Namespace namespace = new Namespace("type", "bucket");
    

    @Test
    public void testGetCounterOp()
    {

        final long counterValue = 1;
        CounterOp op = new CounterOp(counterValue);
        DtUpdateOperation.Builder operation = new DtUpdateOperation.Builder(namespace);
        RiakDtPB.CounterOp counterOp = operation.getCounterOp(op);

        assertEquals(counterValue, counterOp.getIncrement());

    }

    @Test
    public void testGetSetOp()
    {

        BinaryValue addition = BinaryValue.create("1");
        BinaryValue removal = BinaryValue.create("2");

        SetOp op = new SetOp()
            .add(addition)
            .remove(removal);

        DtUpdateOperation.Builder operation = new DtUpdateOperation.Builder(namespace);
        RiakDtPB.SetOp setOp = operation.getSetOp(op);

        BinaryValue serializedAddition = BinaryValue.unsafeCreate(setOp.getAdds(0).toByteArray());
        BinaryValue serializedRemoval = BinaryValue.unsafeCreate(setOp.getRemoves(0).toByteArray());

        assertEquals(addition, serializedAddition);
        assertEquals(removal, serializedRemoval);

    }

    @Test
    public void testGetFlagOp()
    {

        final boolean enabled = true;

        FlagOp op = new FlagOp(enabled);

        DtUpdateOperation.Builder operation = new DtUpdateOperation.Builder(namespace);
        RiakDtPB.MapUpdate.FlagOp flagOp = operation.getFlagOp(op);

        assertEquals(RiakDtPB.MapUpdate.FlagOp.ENABLE, flagOp);
 
    }

    @Test
    public void testGetRegisterOp()
    {

        final BinaryValue registerValue = BinaryValue.create("value");

        RegisterOp op = new RegisterOp(registerValue);

        DtUpdateOperation.Builder operation = new DtUpdateOperation.Builder(namespace);
        ByteString registerOp = operation.getRegisterOp(op);

        ByteString serializedValue = ByteString.copyFrom(registerValue.unsafeGetValue());
        assertEquals(serializedValue, registerOp);
    }

    @Test
    public void testGetMapOpUpdates()
    {

        BinaryValue counterKey = BinaryValue.create("counter");
        BinaryValue setKey = BinaryValue.create("set");
        BinaryValue flagKey = BinaryValue.create("flag");
        BinaryValue registerKey = BinaryValue.create("register");
        BinaryValue mapKey = BinaryValue.create("map");

        BinaryValue setAddValue = BinaryValue.create("1");

        BinaryValue registerValue = BinaryValue.create("value");

        BinaryValue mapAddValue = BinaryValue.create("value");

        MapOp op = new MapOp()
            .update(counterKey, new CounterOp(1))
            .update(setKey, new SetOp().add(setAddValue))
            .update(flagKey, new FlagOp(true))
            .update(registerKey, new RegisterOp(registerValue))
            .update(mapKey, new MapOp());

        DtUpdateOperation.Builder operation = new DtUpdateOperation.Builder(namespace);
        RiakDtPB.MapOp mapOp = operation.getMapOp(op);

        assertTrue(mapOp.getUpdatesCount() == 5);

    }

    @Test
    public void testGetMapOpUpdateNestedMaps()
    {

        BinaryValue key1 = BinaryValue.create("key1");
        MapOp op1 = new MapOp().update(key1, new CounterOp(1));

        BinaryValue key2 = BinaryValue.create("key2");
        MapOp op2 = new MapOp().update(key2, op1);

        BinaryValue key3 = BinaryValue.create("key3");
        MapOp op3 = new MapOp().update(key3, op2);

        DtUpdateOperation.Builder operation = new DtUpdateOperation.Builder(namespace);
        RiakDtPB.MapOp mapOp = operation.getMapOp(op3);

        assertTrue(mapOp.getUpdatesCount() == 1);
        assertTrue(mapOp.getRemovesCount() == 0);

        //op3
        RiakDtPB.MapUpdate update = mapOp.getUpdates(0);
        assertTrue(update.hasMapOp());

        RiakDtPB.MapOp serializedMapOp = update.getMapOp();
        assertTrue(serializedMapOp.getUpdatesCount() == 1);

        //op2
        update = serializedMapOp.getUpdates(0);
        assertTrue(update.hasMapOp());

        serializedMapOp = update.getMapOp();
        assertTrue(serializedMapOp.getUpdatesCount() == 1);

        //op1
        update = serializedMapOp.getUpdates(0);
        assertTrue(update.hasCounterOp());

        RiakDtPB.CounterOp serializedCounterOp = update.getCounterOp();
        assertEquals(1, serializedCounterOp.getIncrement());

    }

}
