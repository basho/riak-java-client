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

public class ListKeysOperation extends FutureOperation<List<ByteArrayWrapper>, RiakKvPB.RpbListKeysResp>
{

    private final Integer timeout;
    private final ByteArrayWrapper bucket;

    public ListKeysOperation(ByteArrayWrapper bucket, Integer timeout)
    {

        if ((null == bucket) || bucket.length() == 0)
        {
            throw new IllegalArgumentException("Bucket can not be null or empty");
        }

        if (timeout < 0)
        {
            throw new IllegalArgumentException("Negative timeout values not allowed");
        }

        this.timeout = timeout;
        this.bucket = bucket;
    }

    public ListKeysOperation(ByteArrayWrapper bucket)
    {
        if ((null == bucket) || bucket.length() == 0)
        {
            throw new IllegalArgumentException("Bucket can not be null or empty");
        }

        this.timeout = null;
        this.bucket = bucket;
    }

    @Override
    protected List<ByteArrayWrapper> convert(List<RiakKvPB.RpbListKeysResp> rawResponse) throws ExecutionException
    {
        List<ByteArrayWrapper> keys = new ArrayList<ByteArrayWrapper>(rawResponse.size());
        for (RiakKvPB.RpbListKeysResp resp : rawResponse)
        {
            for (ByteString bucket : resp.getKeysList())
            {
                keys.add(ByteArrayWrapper.unsafeCreate(bucket.toByteArray()));
            }
        }
        return keys;
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakKvPB.RpbListKeysReq.Builder request = RiakKvPB.RpbListKeysReq.newBuilder();

        request.setBucket(ByteString.copyFrom(bucket.unsafeGetValue()));
        if (timeout != null)
        {
            request.setTimeout(timeout);
        }

        return new RiakMessage(RiakMessageCodes.MSG_ListKeysReq, request.build().toByteArray());
    }

    @Override
    protected RiakKvPB.RpbListKeysResp decode(RiakMessage rawMessage)
    {
        try
        {
            Operations.checkMessageType(rawMessage, RiakMessageCodes.MSG_ListKeysResp);
            return RiakKvPB.RpbListKeysResp.parseFrom(rawMessage.getData());
        }
        catch (InvalidProtocolBufferException e)
        {
            throw new IllegalArgumentException("Invalid message received", e);
        }
    }

    @Override
    protected boolean done(RiakKvPB.RpbListKeysResp message)
    {
        return message.getDone();
    }
}
