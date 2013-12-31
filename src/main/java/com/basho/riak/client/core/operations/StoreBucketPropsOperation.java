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
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakPB;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
//TODO: return some sort of "success" instead of Void
public class StoreBucketPropsOperation extends FutureOperation<Boolean, Void>
{

    private final RiakPB.RpbSetBucketReq.Builder reqBuilder;

    private StoreBucketPropsOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
    }

    @Override
    protected Boolean convert(List<Void> rawResponse) throws ExecutionException
    {
        return true;
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakPB.RpbSetBucketReq req = reqBuilder.build();
        return new RiakMessage(RiakMessageCodes.MSG_SetBucketReq, req.toByteArray());
    }

    @Override
    protected Void decode(RiakMessage rawMessage)
    {
        Operations.checkMessageType(rawMessage, RiakMessageCodes.MSG_SetBucketResp);
        return null;
    }

    public static class Builder extends StoreBucketTypePropsOperation.PropsBuilder<Builder>
    {
        private final RiakPB.RpbSetBucketReq.Builder reqBuilder
            = RiakPB.RpbSetBucketReq.newBuilder();
        
        public Builder(ByteArrayWrapper bucketName)
        {
            if (null == bucketName || bucketName.length() == 0)
            {
                throw new IllegalArgumentException("Bucket name can not be null or zero length");
            }
            reqBuilder.setBucket(ByteString.copyFrom(bucketName.unsafeGetValue()));
        }
        
        /**
         * Set the bucket type. If unset "default" is used.
         *
         * @param bucketType the bucket type to use
         * @return A reference to this object.
         */
        public Builder withBucketType(ByteArrayWrapper bucketType)
        {
            if (null == bucketType || bucketType.length() == 0)
            {
                throw new IllegalArgumentException("Bucket type can not be null or zero length");
            }
            reqBuilder.setType(ByteString.copyFrom(bucketType.unsafeGetValue()));
            return this;
        }
        
        @Override
        protected Builder self()
        {
            return this;
        }
        
        public StoreBucketPropsOperation build()
        {
            reqBuilder.setProps(propsBuilder);
            return new StoreBucketPropsOperation(this);
        }
        
    }
}
