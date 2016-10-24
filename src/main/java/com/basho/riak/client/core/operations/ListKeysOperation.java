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
import java.util.Iterator;
import java.util.List;

public class ListKeysOperation extends StreamingFutureOperation<ListKeysOperation.Response, RiakKvPB.RpbListKeysResp, Namespace>
{
    private final Namespace namespace;
    private final RiakKvPB.RpbListKeysReq.Builder reqBuilder;


    private ListKeysOperation(Builder builder)
    {
        super(builder.streamResults);
        this.reqBuilder = builder.reqBuilder;
        this.namespace = builder.namespace;
    }

    @Override
    protected Response convert(List<RiakKvPB.RpbListKeysResp> rawResponse)
    {
        Response.Builder builder = new Response.Builder();
        for (RiakKvPB.RpbListKeysResp resp : rawResponse)
        {
            builder.addKeys(convertSingleResponse(resp));
        }
        return builder.build();
    }

    private List<BinaryValue> convertSingleResponse(RiakKvPB.RpbListKeysResp resp)
    {
        List<BinaryValue> keys = new ArrayList<>(resp.getKeysCount());

        for (ByteString key : resp.getKeysList())
        {
            keys.add(BinaryValue.unsafeCreate(key.toByteArray()));
        }

        return keys;
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
    protected boolean done(RiakKvPB.RpbListKeysResp message)
    {
        return message.getDone();
    }

    @Override
    public Namespace getQueryInfo()
    {
        return namespace;
    }

    @Override
    protected Response processStreamingChunk(RiakKvPB.RpbListKeysResp rawResponseChunk)
    {
        return new Response.Builder().addKeys(convertSingleResponse(rawResponseChunk)).build();
    }

    public static class Builder
    {
        private final RiakKvPB.RpbListKeysReq.Builder reqBuilder =
            RiakKvPB.RpbListKeysReq.newBuilder();
        private final Namespace namespace;
        private boolean streamResults;

        /**
         * Construct a builder for a ListKeysOperaiton.
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

        /**
         * Set the streamResults flag.
         *
         * If unset or false, the entire result set will be available through the {@link ListKeysOperation#get()}
         * method once the operation is complete.
         *
         * If set to true, results will be pushed to the queue available through the {@link ListKeysOperation#getResultsQueue()}
         * method as soon as they are available.
         *
         * @param streamResults whether to stream results to {@link ListKeysOperation#get()}(false), or {@link ListKeysOperation#getResultsQueue()}(true)
         * @return A reference to this object.
         */
        public Builder streamResults(boolean streamResults)
        {
            this.streamResults = streamResults;
            return this;
        }

        public ListKeysOperation build()
        {
            return new ListKeysOperation(this);
        }
    }

    public static class Response implements Iterable<BinaryValue>
    {
        private final List<BinaryValue> keys;
        private Response(Builder builder)
        {
            this.keys = builder.keys;
        }

        public List<BinaryValue> getKeys()
        {
            return keys;
        }

        @Override
        public Iterator<BinaryValue> iterator()
        {
            return keys.iterator();
        }

        static class Builder
        {
            private List<BinaryValue> keys = new ArrayList<>();

            Builder addKeys(List<BinaryValue> keys)
            {
                this.keys.addAll(keys);
                return this;
            }

            Builder addKey(BinaryValue key)
            {
                this.keys.add(key);
                return this;
            }

            Response build()
            {
                return new Response(this);
            }
        }
    }
}
