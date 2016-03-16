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

import com.basho.riak.client.api.commands.datatypes.UpdateMap;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.core.operations.DtUpdateOperation;
import com.basho.riak.client.api.commands.datatypes.UpdateDatatype.Option;
import com.basho.riak.client.api.commands.datatypes.Context;
import com.basho.riak.client.api.commands.datatypes.MapUpdate;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.crdt.types.RiakMap;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakDtPB;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class UpdateDatatypeTest extends MockedResponseOperationTest<DtUpdateOperation, DtUpdateOperation.Response>
{
    @Mock
    private Context context;
    private final Location key = new Location(new Namespace("type","bucket"), "key");

    public UpdateDatatypeTest()
    {
        super(DtUpdateOperation.Response.class);
    }

    @Override
    protected void setupResponse(DtUpdateOperation.Response mockedResponse)
    {
        super.setupResponse(mockedResponse);

        when(mockedResponse.getCrdtElement()).thenReturn(new RiakMap(new ArrayList<RiakMap.MapEntry>()));
        when(mockedResponse.getContext()).thenReturn(BinaryValue.create(new byte[]{'1'}));

        when(context.getValue()).thenReturn(BinaryValue.unsafeCreate(new byte[] {'1'}));
    }

    @Test
    public void testStore() throws ExecutionException, InterruptedException
    {

        MapUpdate update = new MapUpdate();

        UpdateMap store = new UpdateMap.Builder(key, update)
	        .withContext(context)
            .withOption(Option.DW, new Quorum(1))
            .withOption(Option.PW, new Quorum(1))
            .withOption(Option.N_VAL, 1)
            .withOption(Option.RETURN_BODY, true)
            .withOption(Option.SLOPPY_QUORUM, true)
            .withOption(Option.TIMEOUT, 1000)
            .withOption(Option.W, new Quorum(1))
	        .build();

        final DtUpdateOperation operation = executeAndVerify(store);

        final RiakDtPB.DtUpdateReq.Builder builder =
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
