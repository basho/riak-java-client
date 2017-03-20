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

import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakPB;
import com.google.protobuf.ByteString;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class StoreBucketPropsOperation extends StorePropertiesOperation<Namespace>
{
    private final RiakPB.RpbSetBucketReq.Builder reqBuilder;

    private StoreBucketPropsOperation(Builder builder)
    {
        super(builder.namespace);
        this.reqBuilder = builder.reqBuilder;
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
        Operations.checkPBMessageType(rawMessage, RiakMessageCodes.MSG_SetBucketResp);
        return null;
    }

    public static class Builder extends PropsBuilder<Builder>
    {
        private final RiakPB.RpbSetBucketReq.Builder reqBuilder
            = RiakPB.RpbSetBucketReq.newBuilder();
        private final Namespace namespace;

        /**
         * Constructs a builder for a StoreBucketPropsOperation.
         * @param namespace The namespace in Riak.
         */
        public Builder(Namespace namespace)
        {
            if (namespace == null)
            {
                throw new IllegalArgumentException("Namespace cannot be null");
            }
            reqBuilder.setBucket(ByteString.copyFrom(namespace.getBucketName().unsafeGetValue()));
            reqBuilder.setType(ByteString.copyFrom(namespace.getBucketType().unsafeGetValue()));
            this.namespace = namespace;
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
