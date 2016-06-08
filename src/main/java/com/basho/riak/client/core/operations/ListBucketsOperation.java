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

import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.core.StreamingFutureOperation;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ListBucketsOperation extends StreamingFutureOperation<ListBucketsOperation.Response,
                                                                   BinaryValue,
                                                                   RiakKvPB.RpbListBucketsResp,
                                                                   BinaryValue>
{
    private final RiakKvPB.RpbListBucketsReq.Builder reqBuilder;
    private final BinaryValue bucketType;
    private final BlockingQueue<BinaryValue> responseQueue;

    private ListBucketsOperation(Builder builder)
    {
        super(builder.streamResults);
        this.reqBuilder = builder.reqBuilder;
        this.bucketType = builder.bucketType;
        this.responseQueue = new LinkedBlockingQueue<>();
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
    protected void processStreamingChunk(RiakKvPB.RpbListBucketsResp rawResponseChunk)
    {
        for (ByteString bucket : rawResponseChunk.getBucketsList())
        {
            final BinaryValue value = BinaryValue.unsafeCreate(bucket.toByteArray());
            this.responseQueue.add(value);
        }
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
            Operations.checkPBMessageType(rawMessage, RiakMessageCodes.MSG_ListBucketsResp);
            return RiakKvPB.RpbListBucketsResp.parseFrom(rawMessage.getData());
        }
        catch (InvalidProtocolBufferException e)
        {
            throw new IllegalArgumentException("Invalid message received", e);
        }

    }

    @Override
    public BinaryValue getQueryInfo()
    {
        return bucketType;
    }

    @Override
    public BlockingQueue<BinaryValue> getResultsQueue()
    {
        return this.responseQueue;
    }

    public static class Builder
    {
        private final RiakKvPB.RpbListBucketsReq.Builder reqBuilder = RiakKvPB.RpbListBucketsReq.newBuilder().setStream(
                true);
        private boolean streamResults = false;
        private BinaryValue bucketType = BinaryValue.create(Namespace.DEFAULT_BUCKET_TYPE);

        /**
         * Create a Builder for a ListBucketsOperation.
         */
        public Builder()
        {}

        /**
         * Provide a timeout for this operation.
         *
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
         * If unset {@link Namespace#DEFAULT_BUCKET_TYPE} is used.
         *
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

        /**
         * Set the streamResults flag.
         *
         * If unset or false, the entire result set will be available through the {@link ListBucketsOperation#get()}
         * method once the operation is complete.
         *
         * If set to true, results will be pushed to the queue available through the {@link ListBucketsOperation#getResultsQueue()}
         * method as soon as they are available.
         *
         * @param streamResults whether to stream results to {@link ListBucketsOperation#get()}(false), or {@link ListBucketsOperation#getResultsQueue()}(true)
         * @return A reference to this object.
         */
        public Builder streamResults(boolean streamResults)
        {
            this.streamResults = streamResults;
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
