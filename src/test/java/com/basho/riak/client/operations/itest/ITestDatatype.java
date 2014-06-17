package com.basho.riak.client.operations.itest;

import com.basho.riak.client.operations.datatypes.CounterUpdate;
import com.basho.riak.client.operations.datatypes.MapUpdate;
import com.basho.riak.client.operations.datatypes.RegisterUpdate;
import com.basho.riak.client.operations.datatypes.FlagUpdate;
import com.basho.riak.client.operations.datatypes.SetUpdate;
import com.basho.riak.client.operations.FetchMap;
import com.basho.riak.client.operations.UpdateDatatype.Option;
import com.basho.riak.client.RiakClient;
import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.operations.UpdateMap;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.Namespace;
import com.basho.riak.client.query.crdt.types.*;
import com.basho.riak.client.util.BinaryValue;
import org.junit.Assume;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import java.util.concurrent.ExecutionException;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ITestDatatype extends ITestBase
{

	private final String numLogins = "logins";
	private final String lastLoginTime = "last-login";
	private final ByteBuffer nowBinary = ByteBuffer.allocate(8).putLong(System.currentTimeMillis());
	private final byte[] now = nowBinary.array();
	private final String username = "username";
	private final String loggedIn = "logged-in";
	private final String shoppingCart = "cart";

    private final Namespace carts = new Namespace(mapBucketType, bucketName);

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
		Set<BinaryValue> expectedSet = new HashSet<BinaryValue>();
		for (int i = 0; i < 10; ++i)
		{
			ByteBuffer b = ByteBuffer.allocate(4);
			b.putInt(i);
			expectedSet.add(BinaryValue.create(b.array()));
		}
		assertTrue(setView.containsAll(expectedSet));
		assertTrue(expectedSet.containsAll(setView));


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

}
