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
package com.basho.riak.client.core.converters;

import com.basho.riak.client.query.crdt.types.*;
import com.basho.riak.client.util.BinaryValue;
import com.basho.riak.protobuf.RiakDtPB;
import com.google.protobuf.ByteString;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class CrdtResponseConverterTest
{

    @Test
    public void testDtUpdateRespCounter()
    {

        RiakDtPB.DtUpdateResp resp = RiakDtPB.DtUpdateResp.newBuilder()
            .setCounterValue(1)
            .build();

        CrdtResponseConverter converter = new CrdtResponseConverter();

        RiakDatatype element = converter.convert(resp);

        assertTrue(element.isCounter());
        assertEquals((Long) 1L, element.getAsCounter().view());

    }

    @Test
    public void testDtUpdateRespSet()
    {

        Set<ByteString> values = new HashSet<ByteString>();
        values.add(ByteString.copyFromUtf8("1"));
        values.add(ByteString.copyFromUtf8("2"));
        values.add(ByteString.copyFromUtf8("3"));

        Set<BinaryValue> wrappedValues = new HashSet<BinaryValue>();
        wrappedValues.add(BinaryValue.create("1"));
        wrappedValues.add(BinaryValue.create("2"));
        wrappedValues.add(BinaryValue.create("3"));

        RiakDtPB.DtUpdateResp resp = RiakDtPB.DtUpdateResp.newBuilder()
            .addAllSetValue(values)
            .build();

        CrdtResponseConverter converter = new CrdtResponseConverter();

        RiakDatatype element = converter.convert(resp);

        assertTrue(element.isSet());
        assertEquals(wrappedValues, element.getAsSet().viewAsSet());

    }

    @Test
    public void testDtUpdateRespMap()
    {

        Set<ByteString> setValues = new HashSet<ByteString>();
        setValues.add(ByteString.copyFromUtf8("1"));
        setValues.add(ByteString.copyFromUtf8("2"));
        setValues.add(ByteString.copyFromUtf8("3"));

        Set<BinaryValue> wrappedSetValues = new HashSet<BinaryValue>();
        wrappedSetValues.add(BinaryValue.create("1"));
        wrappedSetValues.add(BinaryValue.create("2"));
        wrappedSetValues.add(BinaryValue.create("3"));

        final long counterValue = 1;

        final boolean flagValue = true;

        BinaryValue registerValue = BinaryValue.create("stuff");

        BinaryValue counterKey = BinaryValue.create("1");
        BinaryValue setKey = BinaryValue.create("2");
        BinaryValue mapKey = BinaryValue.create("3");
        BinaryValue registerKey = BinaryValue.create("4");
        BinaryValue flagKey = BinaryValue.create("5");

        RiakDtPB.DtUpdateResp resp = RiakDtPB.DtUpdateResp.newBuilder()
            .addMapValue(RiakDtPB.MapEntry.newBuilder()
                .setField(RiakDtPB.MapField.newBuilder()
                    .setType(RiakDtPB.MapField.MapFieldType.COUNTER)
                    .setName(ByteString.copyFrom(counterKey.unsafeGetValue())))
                .setCounterValue(counterValue))
            .addMapValue(RiakDtPB.MapEntry.newBuilder()
                .setField(RiakDtPB.MapField.newBuilder()
                    .setName(ByteString.copyFrom(setKey.unsafeGetValue()))
                    .setType(RiakDtPB.MapField.MapFieldType.SET))
                .addAllSetValue(setValues))
            .addMapValue(RiakDtPB.MapEntry.newBuilder()
                .setField(RiakDtPB.MapField.newBuilder()
                    .setName(ByteString.copyFrom(mapKey.unsafeGetValue()))
                    .setType(RiakDtPB.MapField.MapFieldType.MAP)))
            .addMapValue(RiakDtPB.MapEntry.newBuilder()
                .setField(RiakDtPB.MapField.newBuilder()
                    .setName(ByteString.copyFrom(registerKey.unsafeGetValue()))
                    .setType(RiakDtPB.MapField.MapFieldType.REGISTER))
                .setRegisterValue(ByteString.copyFrom(registerValue.unsafeGetValue())))
            .addMapValue(RiakDtPB.MapEntry.newBuilder()
                .setField(RiakDtPB.MapField.newBuilder()
                    .setName(ByteString.copyFrom(flagKey.unsafeGetValue()))
                    .setType(RiakDtPB.MapField.MapFieldType.FLAG))
                .setFlagValue(flagValue))
            .build();

        CrdtResponseConverter converter = new CrdtResponseConverter();

        RiakDatatype element = converter.convert(resp);

        assertTrue(element.isMap());

        RiakMap crdtMap = element.getAsMap();

        assertTrue(crdtMap.get(counterKey).get(0).isCounter());
        assertTrue(crdtMap.get(setKey).get(0).isSet());
        assertTrue(crdtMap.get(mapKey).get(0).isMap());
        assertTrue(crdtMap.get(registerKey).get(0).isRegister());
        assertTrue(crdtMap.get(flagKey).get(0).isFlag());

        RiakCounter riakCounter = crdtMap.get(counterKey).get(0).getAsCounter();
        assertEquals((Long) counterValue, riakCounter.view());

        RiakSet crdtSet = crdtMap.get(setKey).get(0).getAsSet();
        assertEquals(wrappedSetValues, crdtSet.viewAsSet());

        // the asMap doesn't have any values

        RiakRegister crdtRegister = crdtMap.get(registerKey).get(0).getAsRegister();
        assertEquals(registerValue, crdtRegister.getValue());

        RiakFlag crdtFlag = crdtMap.get(flagKey).get(0).getAsFlag();
        assertEquals(flagValue, crdtFlag.getEnabled());
    }

    @Test
    public void testDtUpdateRespNestedMap()
    {
        BinaryValue mapKey = BinaryValue.create("key");

        RiakDtPB.DtUpdateResp resp = RiakDtPB.DtUpdateResp.newBuilder()
            .addMapValue(RiakDtPB.MapEntry.newBuilder()
                .setField(RiakDtPB.MapField.newBuilder()
                    .setName(ByteString.copyFrom(mapKey.unsafeGetValue()))
                    .setType(RiakDtPB.MapField.MapFieldType.MAP))
                .addMapValue(RiakDtPB.MapEntry.newBuilder()
                    .setField(RiakDtPB.MapField.newBuilder()
                        .setName(ByteString.copyFrom(mapKey.unsafeGetValue()))
                        .setType(RiakDtPB.MapField.MapFieldType.MAP))
                    .addMapValue(RiakDtPB.MapEntry.newBuilder()
                        .setField(RiakDtPB.MapField.newBuilder()
                            .setName(ByteString.copyFrom(mapKey.unsafeGetValue()))
                            .setType(RiakDtPB.MapField.MapFieldType.MAP)))))
            .build();

        CrdtResponseConverter converter = new CrdtResponseConverter();

        RiakDatatype element = converter.convert(resp);

        assertTrue(element.isMap());
        RiakMap map = element.getAsMap();
        assertTrue(map.get(mapKey).get(0).isMap());
        map = map.get(mapKey).get(0).getAsMap();
        assertTrue(map.get(mapKey).get(0).isMap());
        map = map.get(mapKey).get(0).getAsMap();
        assertTrue(map.get(mapKey).get(0).isMap());


    }

}
