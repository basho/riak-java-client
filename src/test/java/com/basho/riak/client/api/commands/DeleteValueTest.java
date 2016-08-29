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

import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.DeleteValue.Option;
import com.basho.riak.client.api.cap.BasicVClock;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.cap.VClock;
import com.basho.riak.client.core.operations.DeleteOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.protobuf.RiakKvPB;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DeleteValueTest extends OperationTestBase<DeleteOperation>
{
    private VClock vClock = new BasicVClock(new byte[]{'1'});
    private Location key = new Location(new Namespace("type","bucket"), "key");

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

       final DeleteOperation operation = executeAndVerify(delete.build());

        final RiakKvPB.RpbDelReq.Builder builder =
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
