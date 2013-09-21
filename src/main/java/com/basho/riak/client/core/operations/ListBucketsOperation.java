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
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ListBucketsOperation extends FutureOperation<List<ByteArrayWrapper>, RiakKvPB.RpbListBucketsResp>
{

    private final Integer timeout;
    private final boolean stream;

    public ListBucketsOperation(int timeout, boolean stream)
    {

        if (timeout < 0)
        {
            throw new IllegalArgumentException("Negative timeout values not allowed");
        }

        this.timeout = timeout;
        this.stream = stream;
    }

    public ListBucketsOperation()
    {
        this.timeout = null;
        this.stream = true;
    }

    @Override
    protected boolean done(RiakKvPB.RpbListBucketsResp message)
    {
        return message.getDone();
    }

    @Override
    protected List<ByteArrayWrapper> convert(List<RiakKvPB.RpbListBucketsResp> rawResponse) throws ExecutionException
    {
        List<ByteArrayWrapper> buckets = new ArrayList<ByteArrayWrapper>(rawResponse.size());
        for (RiakKvPB.RpbListBucketsResp resp : rawResponse)
        {
            for (ByteString bucket : resp.getBucketsList())
            {
                buckets.add(ByteArrayWrapper.unsafeCreate(bucket.toByteArray()));
            }
        }
        return buckets;
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakKvPB.RpbListBucketsReq.Builder request = RiakKvPB.RpbListBucketsReq.newBuilder();

        request.setStream(stream);
        if (timeout != null)
        {
            request.setTimeout(timeout);
        }

        return new RiakMessage(RiakMessageCodes.MSG_ListBucketsReq, request.build().toByteArray());
    }

    @Override
    protected RiakKvPB.RpbListBucketsResp decode(RiakMessage rawMessage)
    {
        try
        {
            Operations.checkMessageType(rawMessage, RiakMessageCodes.MSG_ListBucketsResp);
            return RiakKvPB.RpbListBucketsResp.parseFrom(rawMessage.getData());
        }
        catch (InvalidProtocolBufferException e)
        {
            throw new IllegalArgumentException("Invalid message received", e);
        }

    }
}
