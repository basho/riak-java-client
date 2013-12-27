/*
 * Copyright 2013 Basho Technologies Inc.
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
import com.basho.riak.client.core.converters.BucketPropertiesConverter;
import com.basho.riak.client.query.BucketProperties;
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class FetchBucketTypePropsOperation extends FutureOperation<BucketProperties, RiakPB.RpbGetBucketResp>
{
    private final RiakPB.RpbGetBucketTypeReq.Builder reqBuilder;
    
    public FetchBucketTypePropsOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
    }
    
    @Override
    protected BucketProperties convert(List<RiakPB.RpbGetBucketResp> rawResponse) throws ExecutionException
    {
        // This isn't streaming, there will only be one response. 
        RiakPB.RpbBucketProps pbProps = rawResponse.get(0).getProps();
        return BucketPropertiesConverter.convert(pbProps);
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakPB.RpbGetBucketTypeReq req = reqBuilder.build();
        return new RiakMessage(RiakMessageCodes.MSG_GetBucketTypeReq, req.toByteArray());
    }

    @Override
    protected RiakPB.RpbGetBucketResp decode(RiakMessage rawMessage)
    {
        Operations.checkMessageType(rawMessage, RiakMessageCodes.MSG_GetBucketResp);
        try
        {
            return RiakPB.RpbGetBucketResp.parseFrom(rawMessage.getData());
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new IllegalArgumentException("Invalid message received", ex);
        }
    }
    
    public static class Builder
    {
        private final RiakPB.RpbGetBucketTypeReq.Builder reqBuilder =
            RiakPB.RpbGetBucketTypeReq.newBuilder();
        
        public Builder(ByteArrayWrapper bucketType)
        {
            if (null == bucketType || bucketType.length() == 0)
            {
                throw new IllegalArgumentException("Bucket type can not be null or zero length");
            }
            reqBuilder.setType(ByteString.copyFrom(bucketType.unsafeGetValue()));
        }
        
        public FetchBucketTypePropsOperation build()
        {
            return new FetchBucketTypePropsOperation(this);
        }
        
    }
}
