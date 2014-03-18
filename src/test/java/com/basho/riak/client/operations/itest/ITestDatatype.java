package com.basho.riak.client.operations.itest;

import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.operations.*;
import com.basho.riak.client.operations.datatypes.*;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.crdt.types.*;
import com.basho.riak.client.util.BinaryValue;
import org.junit.Assume;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static junit.framework.Assert.*;

public class ITestDatatype extends ITestBase
{

	private final String numLogins = "logins";
	private final String lastLoginTime = "last-login";
	private final ByteBuffer nowBinary = ByteBuffer.allocate(8).putLong(System.currentTimeMillis());
	private final byte[] now = nowBinary.array();
	private final String username = "username";
	private final String loggedIn = "logged-in";
	private final String shoppingCart = "cart";

	private final Location carts = new Location(bucketName).setBucketType(mapBucketType);

	private final RiakClient client = new RiakClient(cluster);

	@Test
	public void simpleTest() throws ExecutionException, InterruptedException
	{
		Assume.assumeTrue(testCrdt);

		/**
		 * Update some info about a user in a table of users
		 */

		RiakClient client = new RiakClient(cluster);

		resetAndEmptyBucket(new Location(bucketName).setBucketType(mapBucketType));

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
			favorites.add(buffer.array());
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

		UpdateDatatype<RiakMap> update = new UpdateDatatype.Builder<RiakMap>(carts)
			.withUpdate(userEntryUpdate)
			.withOption(DtUpdateOption.RETURN_BODY, true)
			.build();
		UpdateDatatype.Response<RiakMap> updateResponse = client.execute(update);

		FetchMap fetch = new FetchMap.Builder(updateResponse.getKey()).build();
		FetchDatatype.Response<RiakMap> fetchResponse = client.execute(fetch);

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

		resetAndEmptyBucket(new Location(bucketName).setBucketType(mapBucketType));

		// Insert a Map and Counter into logins and observe both counter and map returned
		UpdateDatatype<RiakMap> conflictedUpdateCmd =
			new UpdateDatatype.Builder<RiakMap>(carts)
				.withUpdate(new MapUpdate()
					.addMap(numLogins)
					.addCounter(numLogins))
				.withOption(DtUpdateOption.RETURN_BODY, true)
				.build();

		UpdateDatatype.Response<RiakMap> conflictedResponse =
			client.execute(conflictedUpdateCmd);

		assertNotNull(conflictedResponse.getDatatype());
		assertEquals(2, conflictedResponse.getDatatype().view().size());
		assertNotNull(conflictedResponse.getDatatype().getMap(numLogins));
		assertNotNull(conflictedResponse.getDatatype().getCounter(numLogins));

	}

}
