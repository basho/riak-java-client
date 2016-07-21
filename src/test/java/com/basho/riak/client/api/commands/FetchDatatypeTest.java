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

import com.basho.riak.client.api.commands.datatypes.FetchMap;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.core.operations.DtFetchOperation;
import com.basho.riak.client.api.commands.datatypes.FetchDatatype.Option;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.crdt.types.RiakMap;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakDtPB;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class FetchDatatypeTest extends MockedResponseOperationTest<DtFetchOperation, DtFetchOperation.Response>
{
	private Location key = new Location(new Namespace("type", "bucket"), "key");

    public FetchDatatypeTest()
    {
        super(DtFetchOperation.Response.class);
    }

    @Override
    protected void setupResponse(DtFetchOperation.Response mockedResponse)
    {
        super.setupResponse(mockedResponse);

        when(mockedResponse.getCrdtElement()).thenReturn(new RiakMap(new ArrayList<RiakMap.MapEntry>()));
        when(mockedResponse.getContext()).thenReturn(BinaryValue.create(new byte[]{'1'}));
    }

    @Test
    public void testFetch() throws Exception
    {
        FetchMap fetchValue = new FetchMap.Builder(key)
            .withOption(Option.TIMEOUT, 100)
            .withOption(Option.BASIC_QUORUM, true)
            .withOption(Option.N_VAL, 1)
            .withOption(Option.NOTFOUND_OK, true)
            .withOption(Option.PR, new Quorum(1))
            .withOption(Option.R, new Quorum(1))
            .withOption(Option.SLOPPY_QUORUM, true)
            .withOption(Option.INCLUDE_CONTEXT, true)
	        .build();

        assertEquals(key, fetchValue.getLocation());

        final DtFetchOperation operation = executeAndVerify(fetchValue);
        final RiakDtPB.DtFetchReq.Builder builder =
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
