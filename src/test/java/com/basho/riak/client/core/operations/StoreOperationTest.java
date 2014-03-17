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
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.query.indexes.LongIntIndex;
import com.basho.riak.client.query.indexes.RiakIndexes;
import com.basho.riak.client.query.links.RiakLink;
import com.basho.riak.client.util.BinaryValue;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.assertTrue;

public class StoreOperationTest
{

    @Test
    public void testStoreOperationCreateChannelMessage() throws InvalidProtocolBufferException
    {

        byte[] expectedValue = new byte[]{'O', '_', 'o'};

        BinaryValue bucket = BinaryValue.create("bucket".getBytes());
        BinaryValue key = BinaryValue.create("key".getBytes());

        RiakObject ro = new RiakObject();

        List<RiakLink> links = new ArrayList<RiakLink>();
        links.add(new RiakLink("bucket", "key", "tag"));
        ro.getLinks().addLinks(links);

        RiakIndexes indexes = ro.getIndexes();
        LongIntIndex longIndex = indexes.getIndex(new LongIntIndex.Name("dave"));
        longIndex.add(42L);

        ro.setValue(BinaryValue.unsafeCreate(expectedValue));

        Location location = new Location(bucket).setKey(key);
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
