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
package com.basho.riak.client.core.operations;

import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.indexes.LongIntIndex;
import com.basho.riak.client.core.query.indexes.RiakIndexes;
import com.basho.riak.client.core.query.links.RiakLink;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class StoreOperationTest
{
    @Test
    public void testStoreOperationCreateChannelMessage() throws InvalidProtocolBufferException
    {
        byte[] expectedValue = new byte[]{'O', '_', 'o'};

        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, "bucket");
        BinaryValue key = BinaryValue.create("key".getBytes());

        RiakObject ro = new RiakObject();

        List<RiakLink> links = new ArrayList<>();
        links.add(new RiakLink("bucket", "key", "tag"));
        ro.getLinks().addLinks(links);

        RiakIndexes indexes = ro.getIndexes();
        LongIntIndex longIndex = indexes.getIndex(LongIntIndex.named("dave"));
        longIndex.add(42L);

        ro.setValue(BinaryValue.unsafeCreate(expectedValue));

        Location location = new Location(ns, key);
        StoreOperation operation =
            new StoreOperation.Builder(location)
                .withContent(ro)
                .build();

        RiakMessage rm = operation.createChannelMessage();

        assertTrue(rm.getCode() == RiakMessageCodes.MSG_PutReq);
        RiakKvPB.RpbPutReq req = RiakKvPB.RpbPutReq.parseFrom(rm.getData());
        assertTrue(Arrays.equals(req.getContent().getValue().toByteArray(), expectedValue));
    }
}
