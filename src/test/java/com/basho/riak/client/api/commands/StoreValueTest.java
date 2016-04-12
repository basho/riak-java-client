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
package com.basho.riak.client.api.commands;

import com.basho.riak.client.api.commands.kv.StoreValue.Option;
import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.cap.BasicVClock;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.cap.VClock;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakKvPB;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Ignore;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// TODO: Do something with this. You can't mock the responses because the parents aren't public

@Ignore
public class StoreValueTest
{


	@Mock RiakCluster mockCluster;
	@Mock RiakFuture mockFuture;
	@Mock StoreOperation.Response mockResponse;
	VClock vClock = new BasicVClock(new byte[]{'1'});
	Location key = new Location(new Namespace("type", "bucket"), "key");
	RiakClient client;
	RiakObject riakObject;

	@Before
	@SuppressWarnings("unchecked")
	public void init() throws Exception
	{
		MockitoAnnotations.initMocks(this);
		when(mockResponse.getObjectList()).thenReturn(new ArrayList<RiakObject>());
		when(mockFuture.get()).thenReturn(mockResponse);
		when(mockFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(mockResponse);
		when(mockFuture.isCancelled()).thenReturn(false);
		when(mockFuture.isDone()).thenReturn(true);
		when(mockCluster.execute(any(FutureOperation.class))).thenReturn(mockFuture);
		client = new RiakClient(mockCluster);
		riakObject = new RiakObject();
        riakObject.setVClock(vClock);
		riakObject.setValue(BinaryValue.create(new byte[]{'O', '_', 'o'}));
	}

	@Test
	public void testStore() throws ExecutionException, InterruptedException
	{

		StoreValue.Builder store =
			new StoreValue.Builder(riakObject).withLocation(key)
				.withOption(Option.ASIS, true)
				.withOption(Option.DW, new Quorum(1))
				.withOption(Option.IF_NONE_MATCH, true)
				.withOption(Option.IF_NOT_MODIFIED, true)
				.withOption(Option.PW, new Quorum(1))
				.withOption(Option.N_VAL, 1)
				.withOption(Option.RETURN_BODY, true)
				.withOption(Option.RETURN_HEAD, true)
				.withOption(Option.SLOPPY_QUORUM, true)
				.withOption(Option.TIMEOUT, 1000)
				.withOption(Option.W, new Quorum(1));

		client.execute(store.build());

		ArgumentCaptor<StoreOperation> captor =
			ArgumentCaptor.forClass(StoreOperation.class);
		verify(mockCluster).execute(captor.capture());

		StoreOperation operation = captor.getValue();
		RiakKvPB.RpbPutReq.Builder builder =
			(RiakKvPB.RpbPutReq.Builder) Whitebox.getInternalState(operation, "reqBuilder");

		assertTrue(builder.hasVclock());
		assertEquals(true, builder.getAsis());
		assertEquals(1, builder.getDw());
		assertEquals(true, builder.getIfNotModified());
		assertEquals(true, builder.getIfNoneMatch());
		assertEquals(1, builder.getPw());
		assertEquals(1, builder.getNVal());
		assertEquals(true, builder.getReturnBody());
		assertEquals(true, builder.getReturnHead());
		assertEquals(true, builder.getSloppyQuorum());
		assertEquals(1000, builder.getTimeout());
		assertEquals(1, builder.getW());

	}


}
