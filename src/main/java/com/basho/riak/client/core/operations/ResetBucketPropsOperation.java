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
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakPB;
import com.google.protobuf.ByteString;
import java.util.List;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class ResetBucketPropsOperation extends FutureOperation<Void, Void, Namespace>
{
    private final RiakPB.RpbResetBucketReq.Builder reqBuilder;
    private final Namespace namespace;
    
    private ResetBucketPropsOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
        this.namespace = builder.namespace;
    }
    
    @Override
    protected Void convert(List<Void> rawResponse) 
    {
        return null;
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakPB.RpbResetBucketReq req = 
            reqBuilder.build();
        
        return new RiakMessage(RiakMessageCodes.MSG_ResetBucketReq, req.toByteArray());
    }

    @Override
    protected Void decode(RiakMessage rawMessage)
    {
        Operations.checkMessageType(rawMessage, RiakMessageCodes.MSG_ResetBucketResp);
        return null;
    }

    @Override
    public Namespace getQueryInfo()
    {
        return namespace;
    }
    
    public static class Builder
    {
        private final RiakPB.RpbResetBucketReq.Builder reqBuilder = 
            RiakPB.RpbResetBucketReq.newBuilder();
        private final Namespace namespace;
        
        /**
         * Construct a builder for a ResetBucketPropsOperation. 
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
        
        public ResetBucketPropsOperation build()
        {
            return new ResetBucketPropsOperation(this);
        }
    }
}
