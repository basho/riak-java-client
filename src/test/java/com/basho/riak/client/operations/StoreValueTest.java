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

import com.basho.riak.client.cap.BasicVClock;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.ByteArrayWrapper;
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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StoreValueTest
{


    @Mock RiakCluster mockCluster;
    @Mock RiakFuture mockFuture;
    @Mock StoreOperation.Response mockResponse;
    VClock vClock = new BasicVClock(new byte[]{'1'});
	Location key = new Location("bucket", "key").withType("type");
    RiakClient client;
    RiakObject riakObject;

    @Before
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
        riakObject.setValue(ByteArrayWrapper.create(new byte[]{'O', '_', 'o'}));
    }

    @Test
    public void testStore() throws ExecutionException, InterruptedException
    {

        StoreValue<RiakObject> store = StoreValue.store(key, riakObject, vClock)
            .withOption(StoreOption.ASIS, true)
            .withOption(StoreOption.DW, new Quorum(1))
            .withOption(StoreOption.IF_NONE_MATCH, true)
            .withOption(StoreOption.IF_NOT_MODIFIED, true)
            .withOption(StoreOption.PW, new Quorum(1))
            .withOption(StoreOption.N_VAL, 1)
            .withOption(StoreOption.RETURN_BODY, true)
            .withOption(StoreOption.RETURN_HEAD, true)
            .withOption(StoreOption.SLOPPY_QUORUM, true)
            .withOption(StoreOption.TIMEOUT, 1000)
            .withOption(StoreOption.W, new Quorum(1));

        client.execute(store);

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
