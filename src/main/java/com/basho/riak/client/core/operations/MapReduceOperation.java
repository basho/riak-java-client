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

import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.query.indexes.RawIndex;
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class MapReduceOperation extends FutureOperation<List<ByteArrayWrapper>, RiakKvPB.RpbMapRedResp>
{

    private final ByteArrayWrapper mrFunction;
    private final String contentType;

    public MapReduceOperation(ByteArrayWrapper function, String contentType)
    {

        if ((null == function) || function.length() == 0)
        {
            throw new IllegalArgumentException("Function can not be null or empty");
        }

        if ((null == contentType) || contentType.length() == 0)
        {
            throw new IllegalArgumentException("contentType can not be null or empty");
        }

        this.mrFunction = function;
        this.contentType = contentType;
    }

    @Override
    protected List<ByteArrayWrapper> convert(List<RiakKvPB.RpbMapRedResp> rawResponse) throws ExecutionException
    {
        List<ByteArrayWrapper> results = new ArrayList<ByteArrayWrapper>(rawResponse.size());
        for (RiakKvPB.RpbMapRedResp response : rawResponse) {
            results.add(ByteArrayWrapper.create(response.getResponse().toByteArray()));
        }
        return results;
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakKvPB.RpbMapRedReq request =
            RiakKvPB.RpbMapRedReq.newBuilder()
                .setRequest(ByteString.copyFrom(mrFunction.unsafeGetValue()))
                .setContentType(ByteString.copyFromUtf8(contentType))
                .build();

        return new RiakMessage(RiakMessageCodes.MSG_MapRedReq, request.toByteArray());

    }

    @Override
    protected RiakKvPB.RpbMapRedResp decode(RiakMessage rawMessage)
    {
        Operations.checkMessageType(rawMessage, RiakMessageCodes.MSG_MapRedResp);
        try
        {
            return RiakKvPB.RpbMapRedResp.parseFrom(rawMessage.getData());
        }
        catch (InvalidProtocolBufferException e)
        {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    protected boolean done(RiakKvPB.RpbMapRedResp message)
    {
        return message.getDone();
    }
}
