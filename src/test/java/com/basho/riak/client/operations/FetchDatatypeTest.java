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
import com.basho.riak.client.core.operations.DtFetchOperation;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.crdt.types.RiakMap;
import com.basho.riak.client.util.BinaryValue;
import com.basho.riak.protobuf.RiakDtPB;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FetchDatatypeTest
{

    @Mock RiakCluster mockCluster;
    @Mock RiakFuture mockFuture;
    @Mock DtFetchOperation.Response mockResponse;
	Location key = new Location("bucket").setKey("key").setBucketType("type");
    RiakClient client;

    @Before
    @SuppressWarnings("unchecked")
    public void init() throws Exception
    {
        MockitoAnnotations.initMocks(this);
        when(mockResponse.getCrdtElement()).thenReturn(new RiakMap(new ArrayList<RiakMap.MapEntry>()));
        when(mockResponse.getContext()).thenReturn(BinaryValue.create(new byte[]{'1'}));
        when(mockFuture.get()).thenReturn(mockResponse);
        when(mockFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(mockResponse);
        when(mockFuture.isCancelled()).thenReturn(false);
        when(mockFuture.isDone()).thenReturn(true);
        when(mockFuture.isSuccess()).thenReturn(true);
        when(mockCluster.execute(any(FutureOperation.class))).thenReturn(mockFuture);
        client = new RiakClient(mockCluster);
    }

    @Test
    public void testFetch() throws Exception
    {

        FetchMap fetchValue = new FetchMap.Builder(key)
            .withOption(DtFetchOption.TIMEOUT, 100)
            .withOption(DtFetchOption.BASIC_QUORUM, true)
            .withOption(DtFetchOption.N_VAL, 1)
            .withOption(DtFetchOption.NOTFOUND_OK, true)
            .withOption(DtFetchOption.PR, new Quorum(1))
            .withOption(DtFetchOption.R, new Quorum(1))
            .withOption(DtFetchOption.SLOPPY_QUORUM, true)
            .withOption(DtFetchOption.INCLUDE_CONTEXT, true)
	        .build();

        client.execute(fetchValue);

        ArgumentCaptor<DtFetchOperation> captor =
            ArgumentCaptor.forClass(DtFetchOperation.class);
        verify(mockCluster).execute(captor.capture());

        DtFetchOperation operation = captor.getValue();
        RiakDtPB.DtFetchReq.Builder builder =
            (RiakDtPB.DtFetchReq.Builder) Whitebox.getInternalState(operation, "reqBuilder");

        assertEquals("type", builder.getType().toStringUtf8());
        assertEquals("bucket", builder.getBucket().toStringUtf8());
        assertEquals("key", builder.getKey().toStringUtf8());
        assertEquals(100, builder.getTimeout());
        assertEquals(true, builder.getBasicQuorum());
        assertEquals(1, builder.getNVal());
        assertEquals(true, builder.getNotfoundOk());
        assertEquals(1, builder.getPr());
        assertEquals(1, builder.getR());
        assertEquals(true, builder.getSloppyQuorum());
    }


}
