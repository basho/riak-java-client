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

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.DtUpdateOperation;
import com.basho.riak.client.operations.datatypes.Context;
import com.basho.riak.client.operations.datatypes.MapUpdate;
import com.basho.riak.client.operations.datatypes.RiakMap;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.crdt.types.CrdtMap;
import com.basho.riak.client.util.BinaryValue;
import com.basho.riak.protobuf.RiakDtPB;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UpdateDatatypeTest
{
    @Mock RiakCluster mockCluster;
    @Mock RiakFuture mockFuture;
    @Mock DtUpdateOperation.Response mockResponse;
    @Mock Context context;
    RiakClient client;
	Location key = new Location("bucket").setKey("key").setBucketType("type");

    @Before
    @SuppressWarnings("unchecked")
    public void init() throws Exception
    {
        MockitoAnnotations.initMocks(this);
        when(mockResponse.getCrdtElement()).thenReturn(new CrdtMap(new ArrayList<CrdtMap.MapEntry>()));
        when(mockResponse.getContext()).thenReturn(BinaryValue.create(new byte[]{'1'}));
        when(mockFuture.get()).thenReturn(mockResponse);
        when(mockFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(mockResponse);
        when(mockFuture.isCancelled()).thenReturn(false);
        when(mockFuture.isDone()).thenReturn(true);
        when(mockCluster.execute(any(FutureOperation.class))).thenReturn(mockFuture);
        when(context.getBytes()).thenReturn(new byte[] {'1'});
        client = new RiakClient(mockCluster);
    }

    @Test
    public void testStore() throws ExecutionException, InterruptedException
    {

        MapUpdate update = new MapUpdate();

        UpdateDatatype<RiakMap> store = new UpdateDatatype.Builder<RiakMap>(key)
	        .withContext(context)
	        .withUpdate(update)
            .withOption(DtUpdateOption.DW, new Quorum(1))
            .withOption(DtUpdateOption.PW, new Quorum(1))
            .withOption(DtUpdateOption.N_VAL, 1)
            .withOption(DtUpdateOption.RETURN_BODY, true)
            .withOption(DtUpdateOption.SLOPPY_QUORUM, true)
            .withOption(DtUpdateOption.TIMEOUT, 1000)
            .withOption(DtUpdateOption.W, new Quorum(1))
	        .build();

        client.execute(store);

        ArgumentCaptor<DtUpdateOperation> captor =
            ArgumentCaptor.forClass(DtUpdateOperation.class);
        verify(mockCluster).execute(captor.capture());

        DtUpdateOperation operation = captor.getValue();
        RiakDtPB.DtUpdateReq.Builder builder =
            (RiakDtPB.DtUpdateReq.Builder) Whitebox.getInternalState(operation, "reqBuilder");

        assertEquals(1, builder.getDw());
        assertEquals(1, builder.getPw());
        assertEquals(1, builder.getNVal());
        assertEquals(true, builder.getReturnBody());
        assertEquals(true, builder.getSloppyQuorum());
        assertEquals(1000, builder.getTimeout());
        assertEquals(1, builder.getW());

    }
}
