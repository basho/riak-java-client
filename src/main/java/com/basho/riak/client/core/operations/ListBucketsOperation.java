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
import com.basho.riak.client.query.Location;
import com.basho.riak.client.util.BinaryValue;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.List;

public class ListBucketsOperation extends FutureOperation<ListBucketsOperation.Response, RiakKvPB.RpbListBucketsResp, BinaryValue>
{
    private final RiakKvPB.RpbListBucketsReq.Builder reqBuilder;
    private final BinaryValue bucketType;
    
    private ListBucketsOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
        this.bucketType = builder.bucketType;
    }

    @Override
    protected boolean done(RiakKvPB.RpbListBucketsResp message)
    {
        return message.getDone();
    }

    @Override
    protected ListBucketsOperation.Response convert(List<RiakKvPB.RpbListBucketsResp> rawResponse) 
    {
        List<BinaryValue> buckets = new ArrayList<BinaryValue>(rawResponse.size());
        for (RiakKvPB.RpbListBucketsResp resp : rawResponse)
        {
            for (ByteString bucket : resp.getBucketsList())
            {
                buckets.add(BinaryValue.unsafeCreate(bucket.toByteArray()));
            }
        }
        return new Response(bucketType, buckets);
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        return new RiakMessage(RiakMessageCodes.MSG_ListBucketsReq, reqBuilder.build().toByteArray());
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

    @Override
    protected BinaryValue getQueryInfo()
    {
        return bucketType;
    }
    
    public static class Builder
    {
        private final RiakKvPB.RpbListBucketsReq.Builder reqBuilder = 
            RiakKvPB.RpbListBucketsReq.newBuilder().setStream(true);
        private BinaryValue bucketType = Location.DEFAULT_BUCKET_TYPE;
        
        /**
         * Create a Builder for a ListBucketsOperation.
         */
        public Builder()
        {}
        
        /**
         * Provide a timeout for this operation.
         * @param timeout value in milliseconds
         * @return a reference to this object
         */
        public Builder withTimeout(int timeout)
        {
            if (timeout <= 0)
            {
                throw new IllegalArgumentException("Timeout can not be zero or less.");
            }
            reqBuilder.setTimeout(timeout);
            return this;
        }
        
        /**
        * Set the bucket type.
        * If unset "default" is used. 
        * @param bucketType the bucket type to use
        * @return A reference to this object.
        */
        public Builder withBucketType(BinaryValue bucketType)
        {
            if (null == bucketType || bucketType.length() == 0)
            {
                throw new IllegalArgumentException("Bucket type can not be null or zero length");
            }
            reqBuilder.setType(ByteString.copyFrom(bucketType.unsafeGetValue()));
            this.bucketType = bucketType;
            return this;
        }
        
        public ListBucketsOperation build()
        {
            return new ListBucketsOperation(this);
        }
        
    }
    
    public static class Response
    {
        private final BinaryValue bucketType;
        private final List<BinaryValue> buckets;
        
        Response(BinaryValue bucketType, List<BinaryValue> buckets)
        {
            this.bucketType = bucketType;
            this.buckets = buckets;
        }
        
        public BinaryValue getBucketType()
        {
            return bucketType;
        }
        
        public List<BinaryValue> getBuckets()
        {
            return buckets;
        }
    }
    
}
