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
import com.basho.riak.client.core.util.BinaryValue;
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
public class FetchBucketTypePropsOperation extends FutureOperation<FetchBucketTypePropsOperation.Response, RiakPB.RpbGetBucketResp, BinaryValue>
{
    private final RiakPB.RpbGetBucketTypeReq.Builder reqBuilder;
    private final BinaryValue bucketType;

    public FetchBucketTypePropsOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
        this.bucketType = builder.bucketType;
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
        RiakPB.RpbGetBucketTypeReq req = reqBuilder.build();
        return new RiakMessage(RiakMessageCodes.MSG_GetBucketTypeReq, req.toByteArray());
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
    public BinaryValue getQueryInfo()
    {
        return bucketType;
    }

    public static class Builder
    {
        private final RiakPB.RpbGetBucketTypeReq.Builder reqBuilder =
            RiakPB.RpbGetBucketTypeReq.newBuilder();
        private final BinaryValue bucketType;

        /**
         * Construct a builder for a FetchBucketTypePropsOperation.
         * @param bucketType The bucket type in Riak.
         */
        public Builder(BinaryValue bucketType)
        {
            if (bucketType == null)
            {
                throw new IllegalArgumentException("Bucket type cannot be null");
            }
            this.bucketType = bucketType;
            reqBuilder.setType(ByteString.copyFrom(this.bucketType.unsafeGetValue()));
        }

        /**
         * Construct a builder for a FetchBucketTypePropsOperation.
         * @param bucketType The bucket type in Riak.
         */
        public Builder(String bucketType)
        {
            if (bucketType == null)
            {
                throw new IllegalArgumentException("Bucket type cannot be null");
            }
            this.bucketType = BinaryValue.create(bucketType);
            reqBuilder.setType(ByteString.copyFrom(this.bucketType.unsafeGetValue()));
        }

        public FetchBucketTypePropsOperation build()
        {
            return new FetchBucketTypePropsOperation(this);
        }
    }

    /**
     * Response from Fetching a bucket type's properties.
     */
    public static class Response
    {
        private final BucketProperties props;

        private Response(BucketProperties props)
        {
            this.props = props;
        }

        /**
         * Returns the fetched BucketProperties for the bucket type.
         * @return the BucketProperties.
         */
        public BucketProperties getProperties()
        {
            return props;
        }
    }
}
