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
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.protobuf.RiakKvPB;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;
import org.junit.Ignore;
import static org.mockito.Mockito.*;

// TODO: Do something with this. You can't mock the responses because the parents aren't public

@Ignore
public class FetchValueTest
{
    @Mock RiakCluster mockCluster;
    @Mock RiakFuture mockFuture;
    @Mock FetchOperation.Response mockResponse;
    VClock vClock = new BasicVClock(new byte[]{'1'});
	Location key = new Location("bucket").setKey("key").setBucketType("type");
    RiakClient client;

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
    }

    @Test
    public void testFetch() throws Exception
    {

        FetchValue.Builder fetchValue = new FetchValue.Builder(key)
            .withOption(FetchOption.TIMEOUT, 100)
            .withOption(FetchOption.BASIC_QUORUM, true)
            .withOption(FetchOption.DELETED_VCLOCK, true)
            .withOption(FetchOption.HEAD, true)
            .withOption(FetchOption.IF_MODIFIED, vClock)
            .withOption(FetchOption.N_VAL, 1)
            .withOption(FetchOption.NOTFOUND_OK, true)
            .withOption(FetchOption.PR, new Quorum(1))
            .withOption(FetchOption.R, new Quorum(1))
            .withOption(FetchOption.SLOPPY_QUORUM, true);

        client.execute(fetchValue.build());

        ArgumentCaptor<FetchOperation> captor =
            ArgumentCaptor.forClass(FetchOperation.class);
        verify(mockCluster).execute(captor.capture());

        FetchOperation operation = captor.getValue();
        RiakKvPB.RpbGetReq.Builder builder =
            (RiakKvPB.RpbGetReq.Builder) Whitebox.getInternalState(operation, "reqBuilder");

        assertEquals("type", builder.getType().toStringUtf8());
        assertEquals("bucket", builder.getBucket().toStringUtf8());
        assertEquals("key", builder.getKey().toStringUtf8());
        assertEquals(100, builder.getTimeout());
        assertEquals(true, builder.getBasicQuorum());
        assertEquals(true, builder.getDeletedvclock());
        assertEquals(true, builder.getHead());
        assertEquals(1, builder.getNVal());
        assertEquals(true, builder.getNotfoundOk());
        assertEquals(1, builder.getPr());
        assertEquals(1, builder.getR());
        assertEquals(true, builder.getSloppyQuorum());
    }

}
