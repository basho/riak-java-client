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

import com.basho.riak.client.query.crdt.ops.*;
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.protobuf.RiakDtPB;
import com.google.protobuf.ByteString;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class DtUpdateOperationTest
{

    private final ByteArrayWrapper bucket = ByteArrayWrapper.create("buket");
    private final ByteArrayWrapper type = ByteArrayWrapper.create("type");

    @Test
    public void testGetCounterOp()
    {

        final long counterValue = 1;
        CounterOp op = new CounterOp(counterValue);
        DtUpdateOperation.Builder operation = new DtUpdateOperation.Builder(bucket, type);
        RiakDtPB.CounterOp counterOp = operation.getCounterOp(op);

        assertEquals(counterValue, counterOp.getIncrement());

    }

    @Test
    public void testGetSetOp()
    {

        ByteArrayWrapper addition = ByteArrayWrapper.create("1");
        ByteArrayWrapper removal = ByteArrayWrapper.create("2");

        SetOp op = new SetOp()
            .add(addition)
            .remove(removal);

        DtUpdateOperation.Builder operation = new DtUpdateOperation.Builder(bucket, type);
        RiakDtPB.SetOp setOp = operation.getSetOp(op);

        ByteArrayWrapper serializedAddition = ByteArrayWrapper.unsafeCreate(setOp.getAdds(0).toByteArray());
        ByteArrayWrapper serializedRemoval = ByteArrayWrapper.unsafeCreate(setOp.getRemoves(0).toByteArray());

        assertEquals(addition, serializedAddition);
        assertEquals(removal, serializedRemoval);

    }

    @Test
    public void testGetFlagOp()
    {

        final boolean enabled = true;

        FlagOp op = new FlagOp(enabled);

        DtUpdateOperation.Builder operation = new DtUpdateOperation.Builder(bucket, type);
        RiakDtPB.MapUpdate.FlagOp flagOp = operation.getFlagOp(op);

        assertEquals(RiakDtPB.MapUpdate.FlagOp.ENABLE, flagOp);

    }

    @Test
    public void testGetRegisterOp()
    {

        final ByteArrayWrapper registerValue = ByteArrayWrapper.create("value");

        RegisterOp op = new RegisterOp(registerValue);

        DtUpdateOperation.Builder operation = new DtUpdateOperation.Builder(bucket, type);
        ByteString registerOp = operation.getRegisterOp(op);

        ByteString serializedValue = ByteString.copyFrom(registerValue.unsafeGetValue());
        assertEquals(serializedValue, registerOp);
    }

    @Test
    public void testGetMapOpAdditionsAndRemovals()
    {

        ByteArrayWrapper addKey = ByteArrayWrapper.create("add");
        ByteArrayWrapper removeKey = ByteArrayWrapper.create("remove");

        MapOp op = new MapOp()
            .add(addKey, MapOp.FieldType.COUNTER)
            .remove(removeKey, MapOp.FieldType.COUNTER);

        DtUpdateOperation.Builder operation = new DtUpdateOperation.Builder(bucket, type);
        RiakDtPB.MapOp mapOp = operation.getMapOp(op);

        assertTrue(mapOp.getAddsCount() == 1);
        assertEquals(ByteString.copyFrom(addKey.unsafeGetValue()), mapOp.getAdds(0).getName());
        assertEquals(RiakDtPB.MapField.MapFieldType.COUNTER, mapOp.getAdds(0).getType());

        assertTrue(mapOp.getRemovesCount() == 1);
        assertEquals(ByteString.copyFrom(removeKey.unsafeGetValue()), mapOp.getRemoves(0).getName());
        assertEquals(RiakDtPB.MapField.MapFieldType.COUNTER, mapOp.getRemoves(0).getType());

    }

    @Test
    public void testGetMapOpUpdates()
    {

        ByteArrayWrapper counterKey = ByteArrayWrapper.create("counter");
        ByteArrayWrapper setKey = ByteArrayWrapper.create("asSet");
        ByteArrayWrapper flagKey = ByteArrayWrapper.create("flag");
        ByteArrayWrapper registerKey = ByteArrayWrapper.create("register");
        ByteArrayWrapper mapKey = ByteArrayWrapper.create("asMap");

        ByteArrayWrapper setAddValue = ByteArrayWrapper.create("1");

        ByteArrayWrapper registerValue = ByteArrayWrapper.create("value");

        ByteArrayWrapper mapAddValue = ByteArrayWrapper.create("value");

        MapOp op = new MapOp()
            .update(counterKey, new CounterOp(1))
            .update(setKey, new SetOp().add(setAddValue))
            .update(flagKey, new FlagOp(true))
            .update(registerKey, new RegisterOp(registerValue))
            .update(mapKey, new MapOp()
                .add(mapAddValue, MapOp.FieldType.COUNTER));

        DtUpdateOperation.Builder operation = new DtUpdateOperation.Builder(bucket, type);
        RiakDtPB.MapOp mapOp = operation.getMapOp(op);

        assertTrue(mapOp.getUpdatesCount() == 5);

    }

    @Test
    public void testGetMapOpUpdateNestedMaps()
    {

        ByteArrayWrapper key1 = ByteArrayWrapper.create("key1");
        MapOp op1 = new MapOp().update(key1, new CounterOp(1));

        ByteArrayWrapper key2 = ByteArrayWrapper.create("key2");
        MapOp op2 = new MapOp().update(key2, op1);

        ByteArrayWrapper key3 = ByteArrayWrapper.create("key3");
        MapOp op3 = new MapOp().update(key3, op2);

        DtUpdateOperation.Builder operation = new DtUpdateOperation.Builder(bucket, type);
        RiakDtPB.MapOp mapOp = operation.getMapOp(op3);

        assertTrue(mapOp.getUpdatesCount() == 1);
        assertTrue(mapOp.getAddsCount() == 0);
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
