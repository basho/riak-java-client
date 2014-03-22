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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static junit.framework.Assert.*;
import static org.junit.Assume.assumeTrue;

public class ITestDtUpdateOperation extends ITestBase
{

    private RiakCounter fetchCounter(BinaryValue type, BinaryValue bucket, BinaryValue key)
        throws ExecutionException, InterruptedException
    {
        Location location = new Location(bucket).setBucketType(type).setKey(key);
        DtFetchOperation fetch = new DtFetchOperation.Builder(location).build();
        cluster.execute(fetch);
        DtFetchOperation.Response response = fetch.get();
        RiakDatatype element = response.getCrdtElement();

        assertNotNull(element);
        assertTrue(element.isCounter());

        return element.getAsCounter();
    }

    private RiakSet fetchSet(BinaryValue type, BinaryValue bucket, BinaryValue key)
        throws ExecutionException, InterruptedException
    {
        Location location = new Location(bucket).setBucketType(type).setKey(key);
        DtFetchOperation fetch = new DtFetchOperation.Builder(location).build();

        cluster.execute(fetch);
        DtFetchOperation.Response response = fetch.get();
        RiakDatatype element = response.getCrdtElement();

        assertNotNull(element);
        assertTrue(element.isSet());

        return element.getAsSet();
    }

    private RiakMap fetchMap(BinaryValue type, BinaryValue bucket, BinaryValue key)
        throws ExecutionException, InterruptedException
    {
        Location location = new Location(bucket).setBucketType(type).setKey(key);
        DtFetchOperation fetch = new DtFetchOperation.Builder(location).build();

        cluster.execute(fetch);
        DtFetchOperation.Response response = fetch.get();
        RiakDatatype element = response.getCrdtElement();

        assertNotNull(element);
        assertTrue(element.isMap());

        return element.getAsMap();
    }

    @Test
    public void testCrdtCounter() throws ExecutionException, InterruptedException
    {

        assumeTrue(testCrdt);

        final long iterations = 1;

        BinaryValue key = BinaryValue.create("key");

        resetAndEmptyBucket(new Location(bucketName).setBucketType(counterBucketType));

        RiakCounter counter = fetchCounter(counterBucketType, bucketName, key);
        assertEquals((Long) 0L, counter.view());

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
        assertEquals((Long) iterations, counter.view());

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
        assertEquals((Long) 0L, counter.view());

        resetAndEmptyBucket(new Location(bucketName).setBucketType(counterBucketType));

    }

    @Test
    public void testCrdtSet() throws ExecutionException, InterruptedException
    {

        assumeTrue(testCrdt);

        final int iterations = 1;

        BinaryValue key = BinaryValue.create("key");

        resetAndEmptyBucket(new Location(bucketName).setBucketType(setBucketType));

        RiakSet set = fetchSet(setBucketType, bucketName, key);
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

        RiakSet set = fetchSet(setBucketType, bucketName, key);
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

        RiakMap map = fetchMap(mapBucketType, bucketName, key);

        assertTrue(map.view().isEmpty());

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
        assertEquals(1, map.view().size());
	    assertNotNull(map.view().get(mapKey));
	    assertEquals(1, map.view().get(mapKey).size());
        assertTrue(map.view().get(mapKey).get(0).isSet());
        RiakSet set = map.view().get(mapKey).get(0).getAsSet();
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
        assertEquals(2, map.view().size());
	    assertNotNull(map.view().get(mapKey));
	    assertEquals(1, map.view().get(mapKey).size());
        assertTrue(map.view().get(mapKey).get(0).isCounter());
        RiakCounter counter = map.view().get(mapKey).get(0).getAsCounter();
        assertEquals((Long) 1L, counter.view());


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
        assertEquals(3, map.view().size());
	    assertNotNull(map.view().get(mapKey));
	    assertEquals(1, map.view().get(mapKey).size());
        assertTrue(map.view().get(mapKey).get(0).isFlag());
        RiakFlag flag = map.view().get(mapKey).get(0).getAsFlag();
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
        assertEquals(4, map.view().size());
	    assertNotNull(map.view().get(mapKey));
	    assertEquals(1, map.view().get(mapKey).size());
        RiakRegister register = map.view().get(mapKey).get(0).getAsRegister();
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
        Map<BinaryValue, List<RiakDatatype>> mapView = map.view();
        assertEquals(5, mapView.size());

        assertTrue(mapView.containsKey(mapKey));
	    assertNotNull(map.view().get(mapKey));
	    assertEquals(1, map.view().get(mapKey).size());
        assertTrue(mapView.get(mapKey).get(0).isMap());
        RiakMap nestedMap = mapView.get(mapKey).get(0).getAsMap();
        Map<BinaryValue, List<RiakDatatype>> nestedMapView = nestedMap.view();
        assertEquals(1, nestedMapView.size());

        assertTrue(nestedMapView.containsKey(mapKey));
	    assertNotNull(map.view().get(mapKey));
	    assertEquals(1, map.view().get(mapKey).size());
        assertTrue(nestedMapView.get(mapKey).get(0).isFlag());
        RiakFlag nestedFlag = nestedMapView.get(mapKey).get(0).getAsFlag();
        assertFalse(nestedFlag.getEnabled());

        resetAndEmptyBucket(new Location(bucketName).setBucketType(mapBucketType));

    }

}
