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
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.crdt.ops.*;
import com.basho.riak.client.query.crdt.types.*;
import com.basho.riak.client.util.BinaryValue;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static junit.framework.Assert.*;
import static org.junit.Assume.assumeTrue;

public class ITestDtUpdateOperation extends ITestBase
{

    private CrdtCounter fetchCounter(BinaryValue type, BinaryValue bucket, BinaryValue key)
        throws ExecutionException, InterruptedException
    {
        Location location = new Location(bucket).setBucketType(type).setKey(key);
        DtFetchOperation fetch = new DtFetchOperation.Builder(location).build();
        cluster.execute(fetch);
        DtFetchOperation.Response response = fetch.get();
        CrdtElement element = response.getCrdtElement();

        assertNotNull(element);
        assertTrue(element.isCounter());

        return element.getAsCounter();
    }

    private CrdtSet fetchSet(BinaryValue type, BinaryValue bucket, BinaryValue key)
        throws ExecutionException, InterruptedException
    {
        Location location = new Location(bucket).setBucketType(type).setKey(key);
        DtFetchOperation fetch = new DtFetchOperation.Builder(location).build();

        cluster.execute(fetch);
        DtFetchOperation.Response response = fetch.get();
        CrdtElement element = response.getCrdtElement();

        assertNotNull(element);
        assertTrue(element.isSet());

        return element.getAsSet();
    }

    private CrdtMap fetchMap(BinaryValue type, BinaryValue bucket, BinaryValue key)
        throws ExecutionException, InterruptedException
    {
        Location location = new Location(bucket).setBucketType(type).setKey(key);
        DtFetchOperation fetch = new DtFetchOperation.Builder(location).build();

        cluster.execute(fetch);
        DtFetchOperation.Response response = fetch.get();
        CrdtElement element = response.getCrdtElement();

        assertNotNull(element);
        assertTrue(element.isMap());

        return element.getAsMap();
    }

    @Test
    public void testCrdtCounter() throws ExecutionException, InterruptedException
    {

        assumeTrue(testCrdt);

        final int iterations = 1;

        BinaryValue key = BinaryValue.create("key");

        resetAndEmptyBucket(new Location(bucketName).setBucketType(counterBucketType));

        CrdtCounter counter = fetchCounter(counterBucketType, bucketName, key);
        assertEquals(0, counter.getValue());

        Location location = new Location(bucketName).setBucketType(counterBucketType).setKey(key);
        for (int i = 0; i < iterations; ++i)
        {
            DtUpdateOperation update =
                new DtUpdateOperation.Builder(location)
                    .withOp(new CounterOp(1))
                    .build();

            cluster.execute(update);
            update.get();
        }

        counter = fetchCounter(counterBucketType, bucketName, key);
        assertEquals(iterations, counter.getValue());

        for (int i = 0; i < iterations; ++i)
        {
            DtUpdateOperation update =
                new DtUpdateOperation.Builder(location)
                    .withOp(new CounterOp(-1))
                    .build();

            cluster.execute(update);
            update.get();
        }

        counter = fetchCounter(counterBucketType, bucketName, key);
        assertEquals(0, counter.getValue());

        resetAndEmptyBucket(new Location(bucketName).setBucketType(counterBucketType));

    }

    @Test
    public void testCrdtSet() throws ExecutionException, InterruptedException
    {

        assumeTrue(testCrdt);

        final int iterations = 1;

        BinaryValue key = BinaryValue.create("key");

        resetAndEmptyBucket(new Location(bucketName).setBucketType(setBucketType));

        CrdtSet set = fetchSet(setBucketType, bucketName, key);
        assertTrue(set.viewAsSet().isEmpty());

        Set<BinaryValue> testValues = new HashSet<BinaryValue>(iterations);
        Location location = new Location(bucketName).setBucketType(setBucketType).setKey(key);
        for (int i = 0; i < iterations; ++i)
        {
            ByteBuffer buff = (ByteBuffer) ByteBuffer.allocate(8).putInt(i).rewind();
            BinaryValue wrapped = BinaryValue.create(buff.array());
            testValues.add(wrapped);

            DtUpdateOperation update =
                new DtUpdateOperation.Builder(location)
                    .withOp(new SetOp().add(wrapped))
                    .build();

            cluster.execute(update);
            update.get();
        }

        set = fetchSet(setBucketType, bucketName, key);
        assertEquals(iterations, set.viewAsSet().size());
        assertEquals(testValues, set.viewAsSet());

        for (BinaryValue setElement : testValues)
        {

            DtUpdateOperation update =
                new DtUpdateOperation.Builder(location)
                    .withOp(new SetOp().remove(setElement))
                    .build();

            cluster.execute(update);
            update.get();

        }

        set = fetchSet(setBucketType, bucketName, key);
        assertTrue(set.viewAsSet().isEmpty());

        resetAndEmptyBucket(new Location(bucketName).setBucketType(setBucketType));

    }

    @Test
    public void testCrdtSetInterleved() throws ExecutionException, InterruptedException
    {

        assumeTrue(testCrdt);

        final int iterations = 1;

        BinaryValue key = BinaryValue.create("key");

        resetAndEmptyBucket(new Location(bucketName).setBucketType(setBucketType));

        CrdtSet set = fetchSet(setBucketType, bucketName, key);
        assertTrue(set.viewAsSet().isEmpty());

        Set<BinaryValue> testValues = new HashSet<BinaryValue>(iterations);
        Location location = new Location(bucketName).setBucketType(setBucketType).setKey(key);
        for (int i = 0; i < iterations; ++i)
        {
            ByteBuffer buff = (ByteBuffer) ByteBuffer.allocate(8).putInt(i).rewind();
            BinaryValue wrapped = BinaryValue.create(buff.array());
            testValues.add(wrapped);

            DtUpdateOperation add =
                new DtUpdateOperation.Builder(location)
                    .withOp(new SetOp().add(wrapped))
                    .build();

            cluster.execute(add);
            add.get();

            DtUpdateOperation delete =
                new DtUpdateOperation.Builder(location)
                    .withOp(new SetOp().remove(wrapped))
                    .build();

            cluster.execute(delete);
            delete.get();
        }

        set = fetchSet(setBucketType, bucketName, key);
        assertTrue(set.viewAsSet().isEmpty());

        resetAndEmptyBucket(new Location(bucketName).setBucketType(setBucketType));

    }

    @Test
    public void testCrdtMap() throws ExecutionException, InterruptedException
    {

        assumeTrue(testCrdt);

        BinaryValue key = BinaryValue.create("key");

        resetAndEmptyBucket(new Location(bucketName).setBucketType(mapBucketType));

        CrdtMap map = fetchMap(mapBucketType, bucketName, key);

        assertTrue(map.viewAsMap().isEmpty());

        Location location = new Location(bucketName).setBucketType(mapBucketType).setKey(key);
        BinaryValue setValue = BinaryValue.create("value");
        BinaryValue mapKey = BinaryValue.create("set");
        DtUpdateOperation add =
            new DtUpdateOperation.Builder(location)
                .withOp(new MapOp().add(mapKey, MapOp.FieldType.SET))
                .build();

        cluster.execute(add);
        add.get();

        DtUpdateOperation update =
            new DtUpdateOperation.Builder(location)
                .withOp(new MapOp().update(mapKey, new SetOp().add(setValue)))
                .build();

        cluster.execute(update);
        update.get();

        map = fetchMap(mapBucketType, bucketName, key);
        assertEquals(1, map.viewAsMap().size());
        assertTrue(map.viewAsMap().get(mapKey).isSet());
        CrdtSet set = map.viewAsMap().get(mapKey).getAsSet();
        assertTrue(set.viewAsSet().contains(setValue));


        mapKey = BinaryValue.create("counter");
        add = new DtUpdateOperation.Builder(location)
            .withOp(new MapOp().add(mapKey, MapOp.FieldType.COUNTER))
            .build();

        cluster.execute(add);
        add.get();

        update = new DtUpdateOperation.Builder(location)
            .withOp(new MapOp().update(mapKey, new CounterOp(1)))
            .build();

        cluster.execute(update);
        update.get();

        map = fetchMap(mapBucketType, bucketName, key);
        assertEquals(2, map.viewAsMap().size());
        assertTrue(map.viewAsMap().get(mapKey).isCounter());
        CrdtCounter counter = map.viewAsMap().get(mapKey).getAsCounter();
        assertEquals(1, counter.getValue());


        mapKey = BinaryValue.create("flag");

        DtUpdateOperation addSet =
            new DtUpdateOperation.Builder(location)
                .withOp(new MapOp().add(mapKey, MapOp.FieldType.FLAG))
                .build();

        cluster.execute(addSet);
        addSet.get();

        add =
            new DtUpdateOperation.Builder(location)
                .withOp(new MapOp().update(mapKey, new FlagOp(true)))
                .build();

        cluster.execute(add);
        add.get();

        map = fetchMap(mapBucketType, bucketName, key);
        assertEquals(3, map.viewAsMap().size());
        assertTrue(map.viewAsMap().get(mapKey).isFlag());
        CrdtFlag flag = map.viewAsMap().get(mapKey).getAsFlag();
        assertTrue(flag.getEnabled());


        mapKey = BinaryValue.create("register");
        addSet = new DtUpdateOperation.Builder(location)
            .withOp(new MapOp().add(mapKey, MapOp.FieldType.REGISTER))
            .build();

        cluster.execute(addSet);
        addSet.get();

        add = new DtUpdateOperation.Builder(location)
            .withOp(new MapOp().update(mapKey, new RegisterOp(mapKey)))
            .build();

        cluster.execute(add);
        add.get();

        map = fetchMap(mapBucketType, bucketName, key);
        assertEquals(4, map.viewAsMap().size());
        CrdtRegister register = map.viewAsMap().get(mapKey).getAsRegister();
        assertEquals(mapKey, register.getValue());


        mapKey = BinaryValue.create("map");

        addSet = new DtUpdateOperation.Builder(location)
            .withOp(new MapOp().add(mapKey, MapOp.FieldType.MAP))
            .build();

        cluster.execute(addSet);
        addSet.get();

        add = new DtUpdateOperation.Builder(location)
            .withOp(new MapOp().update(mapKey, new MapOp().add(mapKey, MapOp.FieldType.FLAG)))
            .build();

        cluster.execute(add);
        add.get();

        map = fetchMap(mapBucketType, bucketName, key);
        Map<BinaryValue, CrdtElement> mapView = map.viewAsMap();
        assertEquals(5, mapView.size());

        assertTrue(mapView.containsKey(mapKey));
        assertTrue(mapView.get(mapKey).isMap());
        CrdtMap nestedMap = mapView.get(mapKey).getAsMap();
        Map<BinaryValue, CrdtElement> nestedMapView = nestedMap.viewAsMap();
        assertEquals(1, nestedMapView.size());

        assertTrue(nestedMapView.containsKey(mapKey));
        assertTrue(nestedMapView.get(mapKey).isFlag());
        CrdtFlag nestedFlag = nestedMapView.get(mapKey).getAsFlag();
        assertFalse(nestedFlag.getEnabled());

        resetAndEmptyBucket(new Location(bucketName).setBucketType(mapBucketType));

    }

}
