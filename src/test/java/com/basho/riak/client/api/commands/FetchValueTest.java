/*
 * Copyright 2013-2016 Basho Technologies Inc
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

import com.basho.riak.client.api.commands.kv.FetchValue.Option;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.cap.BasicVClock;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.cap.VClock;
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.protobuf.RiakKvPB;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import org.junit.Ignore;
import static org.mockito.Mockito.*;

// TODO: Do something with this. You can't mock the responses because the parents aren't public

@Ignore
public class FetchValueTest extends MockedResponseOperationTest<FetchOperation, FetchOperation.Response>
{
    public FetchValueTest()
    {
        super(FetchOperation.Response.class);
    }

    private final VClock vClock = new BasicVClock(new byte[]{'1'});
    private final Location key = new Location(new Namespace("type","bucket"), "key");

    @Override
    protected void setupResponse(FetchOperation.Response mockedResponse)
    {
        super.setupResponse(mockedResponse);

        when(mockedResponse.getObjectList()).thenReturn(new ArrayList<>());
    }

    @Test
    public void testFetch() throws Exception
    {
        FetchValue.Builder fetchValue = new FetchValue.Builder(key)
            .withOption(Option.TIMEOUT, 100)
            .withOption(Option.BASIC_QUORUM, true)
            .withOption(Option.DELETED_VCLOCK, true)
            .withOption(Option.HEAD, true)
            .withOption(Option.IF_MODIFIED, vClock)
            .withOption(Option.N_VAL, 1)
            .withOption(Option.NOTFOUND_OK, true)
            .withOption(Option.PR, new Quorum(1))
            .withOption(Option.R, new Quorum(1))
            .withOption(Option.SLOPPY_QUORUM, true);

        final FetchOperation operation = executeAndVerify(fetchValue.build());

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
