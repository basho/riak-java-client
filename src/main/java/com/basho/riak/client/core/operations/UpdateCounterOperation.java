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

import com.basho.riak.client.StoreMeta;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.basho.riak.client.core.operations.Operations.checkMessageType;

/**
 * An operation to update a Riak counter.
 */
public class UpdateCounterOperation extends FutureOperation<Long, RiakKvPB.RpbCounterUpdateResp>
{

    private final ByteArrayWrapper bucket;
    private final ByteArrayWrapper key;
    private final long amount;
    private ByteArrayWrapper bucketType;
    private StoreMeta storeMeta;

    public UpdateCounterOperation(ByteArrayWrapper bucket, ByteArrayWrapper key, long amount)
    {

        if ((null == bucket) || bucket.length() == 0)
        {
            throw new IllegalArgumentException("Bucket can not be null or empty");
        }

        if ((null == key) || key.length() == 0)
        {
            throw new IllegalArgumentException("key can not be null or empty");
        }

        this.bucket = bucket;
        this.key = key;
        this.amount = amount;
    }

    /**
     * Set the bucket type.
     * If unset "default" is used. 
     * @param bucketType the bucket type to use
     * @return A reference to this object.
     */
    public UpdateCounterOperation withBucketType(ByteArrayWrapper bucketType)
    {
        if (null == bucketType || bucketType.length() == 0)
        {
            throw new IllegalArgumentException("Bucket type can not be null or zero length");
        }
        this.bucketType = bucketType;
        return this;
    }
    
    /**
     * The {@link StoreMeta} to use for this fetch operation
     *
     * @param storeMeta
     * @return
     */
    public UpdateCounterOperation withStoreMeta(StoreMeta storeMeta)
    {
        this.storeMeta = storeMeta;
        return this;
    }

    @Override
    protected Long convert(List<RiakKvPB.RpbCounterUpdateResp> responses) throws ExecutionException
    {
        if (responses.size() != 1)
        {
            throw new IllegalArgumentException("Expecting one and only one response to RpcCounterUpdateReq");
        }
        RiakKvPB.RpbCounterUpdateResp response = responses.get(0);
        return response.hasValue() ? response.getValue() : null;
    }

    @Override
    protected RiakKvPB.RpbCounterUpdateResp decode(RiakMessage rawResponse)
    {
        checkMessageType(rawResponse, RiakMessageCodes.MSG_GetCounterResp);
        try
        {
            return RiakKvPB.RpbCounterUpdateResp.parseFrom(rawResponse.getData());
        }
        catch (InvalidProtocolBufferException e)
        {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    protected RiakMessage createChannelMessage()
    {

        RiakKvPB.RpbCounterUpdateReq.Builder builder = RiakKvPB.RpbCounterUpdateReq.newBuilder();
        builder.setBucket(ByteString.copyFrom(bucket.unsafeGetValue()));
        builder.setKey(ByteString.copyFrom(key.unsafeGetValue()));

        if (bucketType != null)
        {
            builder.setType(ByteString.copyFrom(bucketType.unsafeGetValue()));
        }
        
        if (storeMeta.hasW())
        {
            builder.setW(storeMeta.getW().getIntValue());
        }

        if (storeMeta.hasDw())
        {
            builder.setDw(storeMeta.getDw().getIntValue());
        }

        if (storeMeta.hasPw())
        {
            builder.setPw(storeMeta.getPw().getIntValue());
        }

        builder.setAmount(amount);

        RiakKvPB.RpbCounterUpdateReq req = builder.build();
        return new RiakMessage(RiakMessageCodes.MSG_GetCounterResp, req.toByteArray());

    }
}
