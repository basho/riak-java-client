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

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.ConflictResolver;
import com.basho.riak.client.api.cap.ConflictResolverFactory;
import com.basho.riak.client.api.commands.kv.UpdateValue;
import com.basho.riak.client.api.convert.Converter;
import com.basho.riak.client.api.convert.PassThroughConverter;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakFutureListener;
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static org.mockito.Mockito.*;

// TODO: Do something with this. You can't mock the responses because the parents aren't public

public class UpdateValueTest
{
    @Mock
    RiakCluster mockCluster;
    Location key = new Location(new Namespace("type", "bucket"), "key");
    RiakClient client;
    RiakObject riakObject;

    @Ignore
    //@Before
    @SuppressWarnings("unchecked")
    public void init() throws Exception
    {
        MockitoAnnotations.initMocks(this);

        riakObject = new RiakObject();
        riakObject.setValue(BinaryValue.create(new byte[]{'O', '_', 'o'}));

        ArrayList<RiakObject> objects = new ArrayList<>();
        objects.add(riakObject);

        FetchOperation.Response fetchResponse = mock(FetchOperation.Response.class);
        when(fetchResponse.getObjectList()).thenReturn(objects);

        StoreOperation.Response storeResponse = mock(StoreOperation.Response.class);
        when(storeResponse.getObjectList()).thenReturn(objects);

        when(mockCluster.execute(any(FutureOperation.class))).thenReturn(new ImmediateRiakFuture<FetchOperation
                .Response, Location>(
                fetchResponse)).thenReturn(new ImmediateRiakFuture<StoreOperation.Response, Location>(storeResponse));

        client = new RiakClient(mockCluster);

    }


    @Test
    @Ignore
    @SuppressWarnings("unchecked")
    public void testUpdateValue() throws ExecutionException, InterruptedException
    {
        UpdateValue.Update spiedUpdate = spy(new NoopUpdate());
        ConflictResolver<RiakObject> spiedResolver = spy(ConflictResolverFactory.getInstance()
                                                                                .getConflictResolver(RiakObject.class));

        Converter<RiakObject> spiedConverter = spy(new PassThroughConverter());

        UpdateValue.Builder update = new UpdateValue.Builder(key).withUpdate(spiedUpdate);

        client.execute(update.build());

        verify(mockCluster, times(2)).execute(any(FutureOperation.class));
        verify(spiedResolver, times(1)).resolve(anyList());
        verify(spiedUpdate, times(1)).apply(any(RiakObject.class));
        verify(spiedConverter, times(1)).fromDomain(any(RiakObject.class),
                                                    any(Namespace.class),
                                                    any(BinaryValue.class));
        verify(spiedConverter, times(2)).toDomain(any(RiakObject.class), any(Location.class));

    }

    @Test
    public void testHandleFetchFailure() throws UnknownHostException, InterruptedException
    {
        UpdateValue update = new UpdateValue.Builder(key).withUpdate(new NoopUpdate()).build();

        // Setup new client with 0 connections.
        RiakClient client = RiakClient.newClient(1, new ArrayList<>());
        final RiakFuture<UpdateValue.Response, Location> updateFuture = client.executeAsync(update);
        updateFuture.addListener(listenerFuture ->
                                 {
                                     // Assert that we fail
                                     assertNotNull(listenerFuture.cause());
                                     assertFalse(listenerFuture.isSuccess());
                                 });

        updateFuture.await();

        // Whole command should fail too
        assertNotNull(updateFuture.cause());
        assertFalse(updateFuture.isSuccess());
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
