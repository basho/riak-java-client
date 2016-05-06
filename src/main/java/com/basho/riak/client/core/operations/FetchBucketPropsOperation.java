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
import com.basho.riak.client.core.query.BucketProperties;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class FetchBucketPropsOperation extends FutureOperation<FetchBucketPropsOperation.Response, RiakPB.RpbGetBucketResp, Namespace>
{
    private final RiakPB.RpbGetBucketReq.Builder reqBuilder;
    private final Namespace namespace;

    public FetchBucketPropsOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
        this.namespace = builder.namespace;
    }

    @Override
    protected Response convert(List<RiakPB.RpbGetBucketResp> rawResponse)
    {
        // This isn't streaming, there will only be one response.
        RiakPB.RpbBucketProps pbProps = rawResponse.get(0).getProps();
        return new Response(BucketPropertiesConverter.convert(pbProps));
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakPB.RpbGetBucketReq req = reqBuilder.build();
        return new RiakMessage(RiakMessageCodes.MSG_GetBucketReq, req.toByteArray());
    }

    @Override
    protected RiakPB.RpbGetBucketResp decode(RiakMessage rawMessage)
    {
        Operations.checkPBMessageType(rawMessage, RiakMessageCodes.MSG_GetBucketResp);
        try
        {
            return RiakPB.RpbGetBucketResp.parseFrom(rawMessage.getData());
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new IllegalArgumentException("Invalid message received", ex);
        }
    }

    @Override
    public Namespace getQueryInfo()
    {
        return namespace;
    }

    public static class Builder
    {
        private final RiakPB.RpbGetBucketReq.Builder reqBuilder =
            RiakPB.RpbGetBucketReq.newBuilder();
        private final Namespace namespace;

        /**
         * Construct a builder for a FetchBucketPropsOperation.
         * @param namespace The namespace for the bucket.
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

        public FetchBucketPropsOperation build()
        {
            return new FetchBucketPropsOperation(this);
        }
    }

    /**
     * Response from Fetching a bucket's properties.
     */
    public static class Response
    {
        private final BucketProperties props;
        private Response(BucketProperties props)
        {
            this.props = props;
        }

        /**
         * Returns the fetched BucketProperties.
         * @return the BucketProperties.
         */
        public BucketProperties getBucketProperties()
        {
            return props;
        }
    }
}
