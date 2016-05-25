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


public class StreamingListKeysOperation
        extends StreamingFutureOperation<BinaryValue, RiakKvPB.RpbListKeysResp, Namespace>
{
    private final Namespace namespace;
    private final RiakKvPB.RpbListKeysReq.Builder reqBuilder;
    private final BlockingQueue<BinaryValue> responseQueue;

    private StreamingListKeysOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
        this.namespace = builder.namespace;
        this.responseQueue = new LinkedBlockingQueue<>();
    }

    @Override
    protected Void processStreamingChunk(RiakKvPB.RpbListKeysResp rawResponseChunk)
    {
        for (ByteString key : rawResponseChunk.getKeysList())
        {
            final BinaryValue value = BinaryValue.unsafeCreate(key.toByteArray());
            this.responseQueue.add(value);
        }
        return null;
    }

    @Override
    protected boolean done(RiakKvPB.RpbListKeysResp message)
    {
        return message.getDone();
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        return new RiakMessage(RiakMessageCodes.MSG_ListKeysReq, reqBuilder.build().toByteArray());
    }

    @Override
    protected RiakKvPB.RpbListKeysResp decode(RiakMessage rawMessage)
    {
        try
        {
            Operations.checkPBMessageType(rawMessage, RiakMessageCodes.MSG_ListKeysResp);
            return RiakKvPB.RpbListKeysResp.parseFrom(rawMessage.getData());
        }
        catch (InvalidProtocolBufferException e)
        {
            throw new IllegalArgumentException("Invalid message received", e);
        }
    }

    @Override
    public Namespace getQueryInfo()
    {
        return this.namespace;
    }

    @Override
    public BlockingQueue<BinaryValue> getResultsQueue()
    {
        return this.responseQueue;
    }

    public static class Builder
    {
        private final RiakKvPB.RpbListKeysReq.Builder reqBuilder =
                RiakKvPB.RpbListKeysReq.newBuilder();
        private final Namespace namespace;

        /**
         * Construct a builder for a StreamingListKeysOperation.
         *
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

        public Builder withTimeout(int timeout)
        {
            if (timeout <= 0)
            {
                throw new IllegalArgumentException("Timeout can not be zero or less");
            }
            reqBuilder.setTimeout(timeout);
            return this;
        }

        public StreamingListKeysOperation build()
        {
            return new StreamingListKeysOperation(this);
        }
    }
}
