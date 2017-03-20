/*
 * Copyright Basho Technologies Inc.
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
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakPB;
import com.google.protobuf.ByteString;

/**
 *
 * @author Luke Bakken <lbakken@basho.com>
 * @since 2.2
 */
public class StoreBucketTypePropsOperation extends StorePropertiesOperation<BinaryValue>
{
    private final RiakPB.RpbSetBucketTypeReq.Builder reqBuilder;

    private StoreBucketTypePropsOperation(Builder builder)
    {
        super(builder.bucketType);
        this.reqBuilder = builder.reqBuilder;
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakPB.RpbSetBucketTypeReq req = reqBuilder.build();
        return new RiakMessage(RiakMessageCodes.MSG_SetBucketTypeReq, req.toByteArray());
    }

    @Override
    protected Void decode(RiakMessage rawMessage)
    {
        Operations.checkPBMessageType(rawMessage, RiakMessageCodes.MSG_SetBucketResp);
        return null;
    }

    public static class Builder extends PropsBuilder<Builder>
    {
        private final RiakPB.RpbSetBucketTypeReq.Builder reqBuilder
            = RiakPB.RpbSetBucketTypeReq.newBuilder();
        private final BinaryValue bucketType;

        /**
         * Constructs a builder for a StoreBucketPropsOperation.
         * @param bucketType The bucket type in Riak
         */
        public Builder(BinaryValue bucketType)
        {
            if (bucketType == null)
            {
                throw new IllegalArgumentException("BucketType cannot be null");
            }
            this.bucketType = bucketType;
            reqBuilder.setType(ByteString.copyFrom(this.bucketType.unsafeGetValue()));
        }

        /**
         * Constructs a builder for a StoreBucketPropsOperation.
         * @param bucketType The bucket type in Riak.
         */
        public Builder(String bucketType)
        {
            if (bucketType == null)
            {
                throw new IllegalArgumentException("BucketType cannot be null");
            }
            this.bucketType = BinaryValue.create(bucketType);
            reqBuilder.setType(ByteString.copyFrom(this.bucketType.unsafeGetValue()));
        }

        @Override
        protected Builder self()
        {
            return this;
        }

        public StoreBucketTypePropsOperation build()
        {
            reqBuilder.setProps(propsBuilder);
            return new StoreBucketTypePropsOperation(this);
        }
    }
}
