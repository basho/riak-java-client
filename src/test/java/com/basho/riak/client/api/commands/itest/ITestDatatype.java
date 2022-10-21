package com.basho.riak.client.api.commands.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.datatypes.*;
import com.basho.riak.client.api.commands.datatypes.UpdateDatatype.Option;
import com.basho.riak.client.core.operations.itest.ITestAutoCleanupBase;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.crdt.types.*;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Assume;
import org.junit.Test;

import javax.swing.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class ITestDatatype extends ITestAutoCleanupBase
{
    private final String numLogins = "logins";
    private final String lastLoginTime = "last-login";
    private final ByteBuffer nowBinary = ByteBuffer.allocate(8).putLong(System.currentTimeMillis());
    private final byte[] now = nowBinary.array();
    private final String username = "username";
    private final String loggedIn = "logged-in";
    private final String shoppingCart = "cart";

    private final Namespace carts = new Namespace(mapBucketType, bucketName);
    private final Namespace uniqueUsersCount = new Namespace(hllBucketType, BinaryValue.create("uniqueUsersCount"));
    private final Namespace uniqueUsers = new Namespace(gsetBucketType, BinaryValue.create("uniqueUsers"));

    private final RiakClient client = new RiakClient(cluster);

    @Test
    public void simpleTest() throws ExecutionException, InterruptedException
    {
        Assume.assumeTrue(testCrdt);

        /**
         * Update some info about a user in a table of users
         */

        resetAndEmptyBucket(carts);

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
            favorites.add(BinaryValue.create(buffer.array()));
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

        UpdateMap update = new UpdateMap.Builder(carts, userEntryUpdate)
            .withOption(Option.RETURN_BODY, true)
            .build();
        UpdateMap.Response updateResponse = client.execute(update);

        Location loc = new Location(carts, updateResponse.getGeneratedKey());
        FetchMap fetch = new FetchMap.Builder(loc).build();
        FetchMap.Response fetchResponse = client.execute(fetch);

        // users
        RiakMap usersMap = fetchResponse.getDatatype();

        // username
        RiakMap usernameMap = usersMap.getMap(username);
        assertNotNull(usernameMap);

        // logins - counter
        RiakCounter numLoginsCounter = usernameMap.getCounter(numLogins);
        assertEquals((Long) 1L, numLoginsCounter.view());

        // last-login - register
        RiakRegister lastLoginTimeRegister = usernameMap.getRegister(lastLoginTime);

        assertTrue(Arrays.equals(now, lastLoginTimeRegister.view().getValue()));

        // logged-in - flag
        RiakFlag loggedInFlag = usernameMap.getFlag(loggedIn);
        assertEquals((Boolean) true, loggedInFlag.view());

        // cart - asSet
        RiakSet shoppingCartSet = usernameMap.getSet(shoppingCart);
        Set<BinaryValue> setView = shoppingCartSet.view();
        Set<BinaryValue> expectedSet = new HashSet<>();
        for (int i = 0; i < 10; ++i)
        {
            ByteBuffer b = ByteBuffer.allocate(4);
            b.putInt(i);
            expectedSet.add(BinaryValue.create(b.array()));
        }
        assertTrue(setView.containsAll(expectedSet));
        assertTrue(expectedSet.containsAll(setView));

        // cart - asSet - item removal
        ByteBuffer b = ByteBuffer.allocate(4);
        b.putInt(5);

        // - remove item '5'
        SetUpdate su            = new SetUpdate().remove(BinaryValue.create(b.array()));
        for (BinaryValue item : expectedSet) {
            System.out.println(" - " + item.getClass());
        }
        System.out.println("\n\n");
//        MapUpdate mu            = new MapUpdate().update(shoppingCart, su);
//        MapUpdate muUser        = new MapUpdate().update(username, mu);
//        UpdateMap updateEntry   = new UpdateMap.Builder(carts, muUser).build();
//        UpdateMap.Response res  = client.execute(updateEntry);

//        // - fetch updated map
//        FetchMap fetchMap                   = new FetchMap.Builder(loc).build();
//        FetchMap.Response fetchMapResponse  = client.execute(fetchMap);
//        RiakMap updatedUsersMap             = fetchMapResponse.getDatatype();
//        RiakMap updatedUsernameMap          = updatedUsersMap.getMap(username);
//        RiakSet updatedShoppingCartSet      = updatedUsernameMap.getSet(shoppingCart);
//        Set<BinaryValue> updatedSetView     = updatedShoppingCartSet.view();
//
//        // - build expected set
//        int[] iArray                        = {0, 1, 2, 3, 4, 6, 7, 8, 9};
//        Set<BinaryValue> updatedExpectedSet = new HashSet<>();
//        for (int i = 0; i < iArray.length; i++) {
//            ByteBuffer buf = ByteBuffer.allocate(4);
//            buf.putInt(iArray[i]);
//            updatedExpectedSet.add(BinaryValue.create(buf.array()));
//        }
//
//        // - compare sets
//        assertTrue(updatedSetView.containsAll(updatedExpectedSet));
//        assertTrue(updatedExpectedSet.containsAll(updatedSetView));
    }

    public void testConflict() throws ExecutionException, InterruptedException
    {
        resetAndEmptyBucket(carts);

        MapUpdate innerMapUpdate = new MapUpdate().update("flag", new FlagUpdate(true));
        CounterUpdate innerCounterUpdate = new CounterUpdate(1);

        // Insert a Map and Counter into logins and observe both counter and map returned
        UpdateMap conflictedUpdateCmd =
            new UpdateMap.Builder(carts, new MapUpdate().update(numLogins, innerMapUpdate).update(numLogins, innerCounterUpdate))
                .withOption(Option.RETURN_BODY, true)
                .build();

        UpdateMap.Response conflictedResponse =
            client.execute(conflictedUpdateCmd);

        assertNotNull(conflictedResponse.getDatatype());
        assertEquals(2, conflictedResponse.getDatatype().view().size());
        assertNotNull(conflictedResponse.getDatatype().getMap(numLogins));
        assertNotNull(conflictedResponse.getDatatype().getCounter(numLogins));
    }

    @Test
    public void testHyperLogLog() throws ExecutionException, InterruptedException
    {
        Assume.assumeTrue(testHllDataType);
        resetAndEmptyBucket(uniqueUsersCount);

        HllUpdate hllUpdate = new HllUpdate().add("user1").add("user2")
                                             .addAll(Arrays.asList("foo", "bar", "baz"))
                                             .add("user1");

        final UpdateHll hllUpdateCmd = new UpdateHll.Builder(uniqueUsersCount, hllUpdate)
                                                                    .withOption(Option.RETURN_BODY, true)
                                                                    .build();

        final UpdateHll.Response hllResponse = client.execute(hllUpdateCmd);

        assertNotNull(hllResponse.getDatatype());
        assertEquals(Long.valueOf(5), hllResponse.getDatatype().view());

        final Location location = new Location(uniqueUsersCount, hllResponse.getGeneratedKey());

        FetchHll hllFetchCmd = new FetchHll.Builder(location).build();
        final RiakHll fetchHll = client.execute(hllFetchCmd);

        assertNotNull(fetchHll);
        assertEquals(5l, fetchHll.getCardinality());
    }

    @Test
    public void testNotFoundHyperLogLog() throws ExecutionException, InterruptedException
    {
        Assume.assumeTrue(testHllDataType);
        resetAndEmptyBucket(uniqueUsersCount);

        final Location location = new Location(uniqueUsersCount, "hll_not_found");

        FetchHll hllFetchCmd = new FetchHll.Builder(location).build();
        final RiakHll fetchHll = client.execute(hllFetchCmd);

        assertNotNull(fetchHll);
        assertEquals(0l, fetchHll.getCardinality());
    }

    @Test
    public void testGSet() throws ExecutionException , InterruptedException
    {
        Assume.assumeTrue(testGSetDataType);
        resetAndEmptyBucket(uniqueUsers);

        final Location location = new Location(uniqueUsers, "site-2017-01-01-" + new Random().nextLong());

        FetchSet fetchSet = new FetchSet.Builder(location).build();
        final FetchSet.Response initialFetchResponse = client.execute(fetchSet);

        final RiakSet initialSet = initialFetchResponse.getDatatype();
        assertTrue(initialSet.view().isEmpty());

        GSetUpdate gsu = new GSetUpdate().add("user1").add("user2").add("user3");
        UpdateSet us = new UpdateSet.Builder(location, gsu).withReturnDatatype(true).build();

        final UpdateSet.Response updateResponse = client.execute(us);
        final Set<BinaryValue> updatedSet = updateResponse.getDatatype().view();

//        assertFalse(updatedSet.isEmpty());
//        assertTrue(updatedSet.contains(BinaryValue.create("user1")));
//        assertTrue(updatedSet.contains(BinaryValue.create("user2")));
//        assertTrue(updatedSet.contains(BinaryValue.create("user3")));
//        assertFalse(updateResponse.hasContext());

//        final FetchSet.Response loadedFetchResponse = client.execute(fetchSet);
//
//        final Set<BinaryValue> loadedSet = loadedFetchResponse.getDatatype().view();
//
//        assertFalse(loadedSet.isEmpty());
//        assertTrue(loadedSet.contains(BinaryValue.create("user1")));
//        assertTrue(loadedSet.contains(BinaryValue.create("user2")));
//        assertTrue(loadedSet.contains(BinaryValue.create("user3")));
//        assertFalse(loadedFetchResponse.hasContext());
    }
}
