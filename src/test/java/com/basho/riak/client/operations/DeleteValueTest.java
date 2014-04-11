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

import com.basho.riak.client.operations.kv.DeleteValue;
import com.basho.riak.client.operations.kv.DeleteValue.Option;
import com.basho.riak.client.RiakClient;
import com.basho.riak.client.cap.BasicVClock;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.DeleteOperation;
import com.basho.riak.client.query.Location;
import com.basho.riak.protobuf.RiakKvPB;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class DeleteValueTest
{

    @Mock RiakCluster mockCluster;
    @Mock RiakFuture mockFuture;
    VClock vClock = new BasicVClock(new byte[]{'1'});
    Location key = new Location("bucket").setKey("key").setBucketType("type");
    RiakClient client;

    @Before
    @SuppressWarnings("unchecked")
    public void init() throws Exception
    {
        MockitoAnnotations.initMocks(this);
        when(mockFuture.get()).thenReturn(null);
        when(mockFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(null);
        when(mockFuture.isCancelled()).thenReturn(false);
        when(mockFuture.isDone()).thenReturn(true);
        when(mockFuture.isSuccess()).thenReturn(true);
        when(mockCluster.<DeleteOperation, Location>execute(any(FutureOperation.class))).thenReturn(mockFuture);
        client = new RiakClient(mockCluster);
    }

    @Test
    public void testDelete() throws Exception
    {
        DeleteValue.Builder delete = new DeleteValue.Builder(key)
	        .withVClock(vClock)
            .withOption(Option.DW, new Quorum(1))
            .withOption(Option.N_VAL, 1)
            .withOption(Option.PR, new Quorum(1))
            .withOption(Option.PW, new Quorum(1))
            .withOption(Option.R, new Quorum(1))
            .withOption(Option.RW, new Quorum(1))
            .withOption(Option.DW, new Quorum(1))
            .withOption(Option.SLOPPY_QUORUM, true)
            .withOption(Option.TIMEOUT, 100)
            .withOption(Option.W, new Quorum(1));


        client.execute(delete.build());

        ArgumentCaptor<DeleteOperation> captor =
            ArgumentCaptor.forClass(DeleteOperation.class);
        verify(mockCluster).execute(captor.capture());

        DeleteOperation operation = captor.getValue();
        RiakKvPB.RpbDelReq.Builder builder =
            (RiakKvPB.RpbDelReq.Builder) Whitebox.getInternalState(operation, "reqBuilder");

        assertTrue(builder.hasVclock());
        assertEquals(1, builder.getDw());
        assertEquals(1, builder.getNVal());
        assertEquals(1, builder.getPr());
        assertEquals(1, builder.getPw());
        assertEquals(1, builder.getR());
        assertEquals(1, builder.getRw());
        assertEquals(1, builder.getDw());
        assertEquals(true, builder.getSloppyQuorum());
        assertEquals(100, builder.getTimeout());
        assertEquals(1, builder.getW());

    }
}
