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
package com.basho.riak.client.operations;

import com.basho.riak.client.cap.ConflictResolver;
import com.basho.riak.client.cap.DefaultResolver;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.convert.PassThroughConverter;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.BinaryValue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class UpdateValueTest
{
	@Mock RiakCluster mockCluster;
	Location key = new Location("bucket", "key").withType("type");
	RiakClient client;
	RiakObject riakObject;

	@Before
	@SuppressWarnings("unchecked")
	public void init() throws Exception
	{
		MockitoAnnotations.initMocks(this);

		riakObject = new RiakObject();
		riakObject.setValue(BinaryValue.create(new byte[]{'O', '_', 'o'}));

		ArrayList<RiakObject> objects = new ArrayList<RiakObject>();
		objects.add(riakObject);

		FetchOperation.Response fetchResponse = mock(FetchOperation.Response.class);
		when(fetchResponse.getObjectList()).thenReturn(objects);

		StoreOperation.Response storeResponse = mock(StoreOperation.Response.class);
		when(storeResponse.getObjectList()).thenReturn(objects);

		when(mockCluster.execute(any(FutureOperation.class)))
			.thenReturn(new ImmediateRiakFuture<FetchOperation.Response>(fetchResponse))
			.thenReturn(new ImmediateRiakFuture<StoreOperation.Response>(storeResponse));

		client = new RiakClient(mockCluster);

	}


	@Test
	@SuppressWarnings("unchecked")
	public void testUpdateValue() throws ExecutionException, InterruptedException
	{
		UpdateValue.Update spiedUpdate = spy(new NoopUpdate());
		ConflictResolver<RiakObject> spiedResolver = spy(new DefaultResolver<RiakObject>());
		Converter<RiakObject> spiedConverter = spy(new PassThroughConverter());

		UpdateValue.Builder update =
			new UpdateValue.Builder<RiakObject>(key)
				.withConverter(spiedConverter)
				.withResolver(spiedResolver)
				.withUpdate(spiedUpdate);

		client.execute(update.build());

		verify(mockCluster, times(2)).execute(any(FutureOperation.class));
		verify(spiedResolver, times(1)).resolve(anyList());
		verify(spiedUpdate, times(1)).apply(any(RiakObject.class));
		verify(spiedConverter, times(1)).fromDomain(any(RiakObject.class));
		verify(spiedConverter, times(2)).toDomain(any(RiakObject.class), any(VClock.class), any(BinaryValue.class));

	}

	private static class NoopUpdate extends UpdateValue.Update<RiakObject>
	{
		@Override
		public RiakObject apply(RiakObject original)
		{
			return original;
		}
	}

}
