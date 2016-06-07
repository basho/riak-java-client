package com.basho.riak.client.core.operations;

import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.core.StreamingFutureOperation;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class StreamingListBucketsOperation
        extends StreamingFutureOperation<BinaryValue, RiakKvPB.RpbListBucketsResp, BinaryValue>
{
    private final BinaryValue bucketType;
    private final RiakKvPB.RpbListBucketsReq.Builder reqBuilder;
    private final BlockingQueue<BinaryValue> responseQueue;

    private StreamingListBucketsOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
        this.bucketType = builder.bucketType;
        this.responseQueue = new LinkedBlockingQueue<>();
    }

    @Override
    protected Void processStreamingChunk(RiakKvPB.RpbListBucketsResp rawResponseChunk)
    {
        for (ByteString bucket : rawResponseChunk.getBucketsList())
        {
            final BinaryValue value = BinaryValue.unsafeCreate(bucket.toByteArray());
            this.responseQueue.add(value);
        }
        return null;
    }

    @Override
    protected boolean done(RiakKvPB.RpbListBucketsResp message)
    {
        return message.getDone();
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
        return this.bucketType;
    }

    @Override
    public BlockingQueue<BinaryValue> getResultsQueue()
    {
        return this.responseQueue;
    }

    public static class Builder
    {
        private final RiakKvPB.RpbListBucketsReq.Builder reqBuilder;
        private BinaryValue bucketType = BinaryValue.create(Namespace.DEFAULT_BUCKET_TYPE);

        /**
         * Create a Builder for a StreamingListBucketsOperation.
         */
        public Builder()
        {
            this.reqBuilder = RiakKvPB.RpbListBucketsReq.newBuilder().setStream(true);
        }

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
         * If unset {@link Namespace#DEFAULT_BUCKET_TYPE} is used.
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

        public StreamingListBucketsOperation build()
        {
            return new StreamingListBucketsOperation(this);
        }
    }
}
