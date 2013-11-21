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
import com.basho.riak.client.util.ByteArrayWrapper;
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

        CrdtElement element = converter.convert(resp);

        assertTrue(element.isCounter());
        assertEquals(1, element.getAsCounter().getValue());

    }

    @Test
    public void testDtUpdateRespSet()
    {

        Set<ByteString> values = new HashSet<ByteString>();
        values.add(ByteString.copyFromUtf8("1"));
        values.add(ByteString.copyFromUtf8("2"));
        values.add(ByteString.copyFromUtf8("3"));

        Set<ByteArrayWrapper> wrappedValues = new HashSet<ByteArrayWrapper>();
        wrappedValues.add(ByteArrayWrapper.create("1"));
        wrappedValues.add(ByteArrayWrapper.create("2"));
        wrappedValues.add(ByteArrayWrapper.create("3"));

        RiakDtPB.DtUpdateResp resp = RiakDtPB.DtUpdateResp.newBuilder()
            .addAllSetValue(values)
            .build();

        CrdtResponseConverter converter = new CrdtResponseConverter();

        CrdtElement element = converter.convert(resp);

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

        Set<ByteArrayWrapper> wrappedSetValues = new HashSet<ByteArrayWrapper>();
        wrappedSetValues.add(ByteArrayWrapper.create("1"));
        wrappedSetValues.add(ByteArrayWrapper.create("2"));
        wrappedSetValues.add(ByteArrayWrapper.create("3"));

        final long counterValue = 1;

        final boolean flagValue = true;

        ByteArrayWrapper registerValue = ByteArrayWrapper.create("stuff");

        ByteArrayWrapper counterKey = ByteArrayWrapper.create("1");
        ByteArrayWrapper setKey = ByteArrayWrapper.create("2");
        ByteArrayWrapper mapKey = ByteArrayWrapper.create("3");
        ByteArrayWrapper registerKey = ByteArrayWrapper.create("4");
        ByteArrayWrapper flagKey = ByteArrayWrapper.create("5");

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

        CrdtElement element = converter.convert(resp);

        assertTrue(element.isMap());

        CrdtMap crdtMap = element.getAsMap();

        assertTrue(crdtMap.get(counterKey).isCounter());
        assertTrue(crdtMap.get(setKey).isSet());
        assertTrue(crdtMap.get(mapKey).isMap());
        assertTrue(crdtMap.get(registerKey).isRegister());
        assertTrue(crdtMap.get(flagKey).isFlag());

        CrdtCounter crdtCounter = crdtMap.get(counterKey).getAsCounter();
        assertEquals(counterValue, crdtCounter.getValue());

        CrdtSet crdtSet = crdtMap.get(setKey).getAsSet();
        assertEquals(wrappedSetValues, crdtSet.viewAsSet());

        // the asMap doesn't have any values

        CrdtRegister crdtRegister = crdtMap.get(registerKey).getAsRegister();
        assertEquals(registerValue, crdtRegister.getValue());

        CrdtFlag crdtFlag = crdtMap.get(flagKey).getAsFlag();
        assertEquals(flagValue, crdtFlag.getEnabled());
    }

    @Test
    public void testDtUpdateRespNestedMap()
    {
        ByteArrayWrapper mapKey = ByteArrayWrapper.create("key");

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

        CrdtElement element = converter.convert(resp);

        assertTrue(element.isMap());
        CrdtMap map = element.getAsMap();
        assertTrue(map.get(mapKey).isMap());
        map = map.get(mapKey).getAsMap();
        assertTrue(map.get(mapKey).isMap());
        map = map.get(mapKey).getAsMap();
        assertTrue(map.get(mapKey).isMap());


    }

}
