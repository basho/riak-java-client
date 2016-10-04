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
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.crdt.ops.*;
import com.basho.riak.client.core.query.crdt.types.*;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

public class ITestDtUpdateOperation extends ITestAutoCleanupBase
{
    private RiakCounter fetchCounter(BinaryValue type, BinaryValue bucket, BinaryValue key)
        throws ExecutionException, InterruptedException
    {
        Location location = new Location(new Namespace(type, bucket), key);
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
        Location location = new Location(new Namespace(type, bucket), key);
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
        Location location = new Location(new Namespace(type, bucket), key);
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

        resetAndEmptyBucket(new Namespace(counterBucketType, bucketName));

        RiakCounter counter = fetchCounter(counterBucketType, bucketName, key);
        assertEquals((Long) 0L, counter.view());

        Location location = new Location(new Namespace(counterBucketType, bucketName), key);
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

        resetAndEmptyBucket(new Namespace(counterBucketType, bucketName));
    }

    @Test
    public void testCrdtSet() throws ExecutionException, InterruptedException
    {
        assumeTrue(testCrdt);

        final int iterations = 1;

        BinaryValue key = BinaryValue.create("key");

        resetAndEmptyBucket(new Namespace(setBucketType, bucketName));

        RiakSet set = fetchSet(setBucketType, bucketName, key);
        assertTrue(set.view().isEmpty());

        Set<BinaryValue> testValues = new HashSet<>(iterations);
        Location location = new Location(new Namespace(setBucketType, bucketName), key);
        BinaryValue ctx = null;
        for (int i = 0; i < iterations; ++i)
        {
            ByteBuffer buff = (ByteBuffer) ByteBuffer.allocate(8).putInt(i).rewind();
            BinaryValue wrapped = BinaryValue.create(buff.array());
            testValues.add(wrapped);

            DtUpdateOperation update =
                new DtUpdateOperation.Builder(location)
                    .withOp(new SetOp().add(wrapped))
                    .withReturnBody(true)
                    .build();

            cluster.execute(update);
            DtUpdateOperation.Response resp = update.get();
            ctx = resp.getContext();
            set = resp.getCrdtElement().getAsSet();
        }

        assertEquals(iterations, set.view().size());
        assertEquals(testValues, set.view());

        for (BinaryValue setElement : testValues)
        {
            DtUpdateOperation update =
                new DtUpdateOperation.Builder(location)
                    .withOp(new SetOp().remove(setElement))
                    .withContext(ctx)
                    .build();

            cluster.execute(update);
            update.get();
        }

        set = fetchSet(setBucketType, bucketName, key);
        assertTrue(set.view().isEmpty());

        resetAndEmptyBucket(new Namespace(setBucketType, bucketName));
    }

    @Test
    public void testCrdtSetInterleved() throws ExecutionException, InterruptedException
    {
        assumeTrue(testCrdt);

        final int iterations = 1;

        BinaryValue key = BinaryValue.create("key");

        resetAndEmptyBucket(new Namespace(setBucketType, bucketName));

        RiakSet set = fetchSet(setBucketType, bucketName, key);
        assertTrue(set.view().isEmpty());

        Set<BinaryValue> testValues = new HashSet<>(iterations);
        Location location = new Location(new Namespace(setBucketType, bucketName), key);
        for (int i = 0; i < iterations; ++i)
        {
            ByteBuffer buff = (ByteBuffer) ByteBuffer.allocate(8).putInt(i).rewind();
            BinaryValue wrapped = BinaryValue.create(buff.array());
            testValues.add(wrapped);

            DtUpdateOperation add =
                new DtUpdateOperation.Builder(location)
                    .withOp(new SetOp().add(wrapped))
                    .withReturnBody(true)
                    .build();

            cluster.execute(add);
            DtUpdateOperation.Response resp = add.get();

            DtUpdateOperation delete =
                new DtUpdateOperation.Builder(location)
                    .withOp(new SetOp().remove(wrapped))
                    .withContext(resp.getContext())
                    .build();

            cluster.execute(delete);
            delete.get();
        }

        set = fetchSet(setBucketType, bucketName, key);
        assertTrue(set.view().isEmpty());

        resetAndEmptyBucket(new Namespace(setBucketType, bucketName));
    }

    @Test
    public void testCrdtMap() throws ExecutionException, InterruptedException
    {
        assumeTrue(testCrdt);

        BinaryValue key = BinaryValue.create("key");

        resetAndEmptyBucket(new Namespace(mapBucketType, bucketName));

        RiakMap map = fetchMap(mapBucketType, bucketName, key);

        assertTrue(map.view().isEmpty());

        Location location = new Location(new Namespace(mapBucketType, bucketName), key);
        BinaryValue setValue = BinaryValue.create("value");
        BinaryValue mapKey = BinaryValue.create("set");

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
        assertTrue(set.view().contains(setValue));

        mapKey = BinaryValue.create("counter");

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

        update =
            new DtUpdateOperation.Builder(location)
                .withOp(new MapOp().update(mapKey, new FlagOp(true)))
                .build();

        cluster.execute(update);
        update.get();

        map = fetchMap(mapBucketType, bucketName, key);
        assertEquals(3, map.view().size());
        assertNotNull(map.view().get(mapKey));
        assertEquals(1, map.view().get(mapKey).size());
        assertTrue(map.view().get(mapKey).get(0).isFlag());
        RiakFlag flag = map.view().get(mapKey).get(0).getAsFlag();
        assertTrue(flag.getEnabled());

        mapKey = BinaryValue.create("register");

        update = new DtUpdateOperation.Builder(location)
            .withOp(new MapOp().update(mapKey, new RegisterOp(mapKey)))
            .build();

        cluster.execute(update);
        update.get();

        map = fetchMap(mapBucketType, bucketName, key);
        assertEquals(4, map.view().size());
        assertNotNull(map.view().get(mapKey));
        assertEquals(1, map.view().get(mapKey).size());
        RiakRegister register = map.view().get(mapKey).get(0).getAsRegister();
        assertEquals(mapKey, register.getValue());

        mapKey = BinaryValue.create("map");

        update = new DtUpdateOperation.Builder(location)
            .withOp(new MapOp().update(mapKey, new MapOp().update(mapKey, new FlagOp(false))))
            .build();

        cluster.execute(update);
        update.get();

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

        resetAndEmptyBucket(new Namespace(mapBucketType, bucketName));
    }

    @Test
    public void testComplexMapUpdate() throws InterruptedException, ExecutionException
    {
        assumeTrue(testCrdt);
        testComplexMapUpdate(false);
    }

    @Test
    public void testComplexMapUpdateWithReturnBody() throws InterruptedException, ExecutionException
    {
        // This test will currently fail as returnbody is broken in some cases in Riak.
        assumeTrue(testCrdt);
        testComplexMapUpdate(true);
    }

    private void testComplexMapUpdate(boolean returnBody) throws InterruptedException, ExecutionException
    {
        /*
            Data structure:

            Riak key = "user-info"
            Crdt Map
              -> "Bob" : map
                  -> "logins"     : counter
                  -> "last-login" : register
                  -> "logged-in"  : flag
                  -> "cart"       : set

         */

        BinaryValue key = BinaryValue.create("user-info2");
        BinaryValue username = BinaryValue.create("Bob");
        BinaryValue logins = BinaryValue.create("logins");
        BinaryValue lastLogin = BinaryValue.create("last-login");
        BinaryValue loggedIn = BinaryValue.create("logged-in");
        BinaryValue cartContents = BinaryValue.create("cart");

        MapOp outerMap = new MapOp();
        MapOp innerMap = new MapOp();

        ByteBuffer nowBinary = ByteBuffer.allocate(8).putLong(System.currentTimeMillis());
        byte[] now = nowBinary.array();

        CounterOp counterOp = new CounterOp(1);
        RegisterOp registerOp = new RegisterOp(BinaryValue.create(now));
        FlagOp flagOp = new FlagOp(false);
        SetOp setOp = new SetOp()
                        .add(BinaryValue.create("Item 1"))
                        .add(BinaryValue.create("Item 2"));

        innerMap.update(logins, counterOp)
                .update(lastLogin, registerOp)
                .update(loggedIn, flagOp)
                .update(cartContents, setOp);

        outerMap.update(username, innerMap);

        Namespace ns = new Namespace(mapBucketType, bucketName);
        Location loc = new Location(ns, key);

        DtUpdateOperation update = new DtUpdateOperation.Builder(loc)
                                        .withOp(outerMap)
                                        .withReturnBody(returnBody)
                                        .build();

        cluster.execute(update);

        update.await();

        if (!update.isSuccess())
        {
            fail("Update operation failed: " + update.cause().toString());
        }

        RiakMap map;
        if (returnBody)
        {
            DtUpdateOperation.Response response = update.get();
            assertNotNull(response);
            assertTrue(response.hasCrdtElement());
            assertTrue(response.hasContext());
            RiakDatatype dt = response.getCrdtElement();
            assertTrue(dt.isMap());
            map = dt.getAsMap();
        }
        else
        {
            map = fetchMap(mapBucketType, bucketName, key);
        }

        map = map.getMap(username);
        assertNotNull(map);
        RiakCounter counter = map.getCounter(logins);
        assertNotNull(counter);
        RiakRegister register = map.getRegister(lastLogin);
        assertNotNull(register);
        RiakFlag flag = map.getFlag(loggedIn);
        assertNotNull(flag);
        RiakSet set = map.getSet(cartContents);
        assertNotNull(set);

        resetAndEmptyBucket(new Namespace(mapBucketType, bucketName));
    }

    @Test
    public void testSimpleMap() throws InterruptedException, ExecutionException
    {
        assumeTrue(testCrdt);

        BinaryValue key = BinaryValue.create("simple-map");
        BinaryValue mapKey = BinaryValue.create("set");
        Namespace ns = new Namespace(mapBucketType, bucketName);
        Location loc = new Location(ns, key);

        SetOp setOp = new SetOp()
                        .add(BinaryValue.create("Item 1"))
                        .add(BinaryValue.create("Item 2"));

        MapOp op = new MapOp().update(mapKey, setOp);

        DtUpdateOperation update = new DtUpdateOperation.Builder(loc)
                                        .withOp(op)
                                        .withReturnBody(true)
                                        .build();

        cluster.execute(update);

        update.await();

        assertTrue(update.isSuccess());
        DtUpdateOperation.Response response = update.get();
        assertNotNull(response);
        assertTrue(response.hasCrdtElement());
        assertTrue(response.hasContext());
        RiakDatatype dt = response.getCrdtElement();
        assertTrue(dt.isMap());
        RiakMap map = dt.getAsMap();

        resetAndEmptyBucket(new Namespace(mapBucketType, bucketName));
    }
}
