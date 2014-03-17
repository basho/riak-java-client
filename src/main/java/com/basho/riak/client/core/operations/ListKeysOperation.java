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
import java.util.concurrent.ExecutionException;

public class ListKeysOperation extends FutureOperation<ListKeysOperation.Response, RiakKvPB.RpbListKeysResp>
{
    private final RiakKvPB.RpbListKeysReq.Builder reqBuilder;
    
    private ListKeysOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
    }

    @Override
    protected Response convert(List<RiakKvPB.RpbListKeysResp> rawResponse) throws ExecutionException
    {
        Response.Builder builder = new Response.Builder();
        for (RiakKvPB.RpbListKeysResp resp : rawResponse)
        {
            for (ByteString bucket : resp.getKeysList())
            {
                builder.addKey(BinaryValue.unsafeCreate(bucket.toByteArray()));
            }
        }
        return builder.build();
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
            Operations.checkMessageType(rawMessage, RiakMessageCodes.MSG_ListKeysResp);
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
    
    public static class Builder
    {
        private final RiakKvPB.RpbListKeysReq.Builder reqBuilder =
            RiakKvPB.RpbListKeysReq.newBuilder();
        private final Location location;
        
        /**
         * Construct a builder for a ListKeysOperaiton.
         * @param location The location in Riak.
         */
        public Builder(Location location)
        {
            if (location == null)
            {
                throw new IllegalArgumentException("Location cannot be null");
            }
            reqBuilder.setBucket(ByteString.copyFrom(location.getBucketName().unsafeGetValue()));
            reqBuilder.setType(ByteString.copyFrom(location.getBucketType().unsafeGetValue()));
            this.location = location;
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
        
        public ListKeysOperation build()
        {
            return new ListKeysOperation(this);
        }
    }
    
    public static class Response extends ResponseWithLocation
    {
        private final List<BinaryValue> keys;
        private Response(Init<?> builder)
        {
            super(builder);
            this.keys = builder.keys;
        }
        
        public List<BinaryValue> getKeys()
        {
            return keys;
        }
        
        protected static abstract class Init<T extends Init<T>> extends ResponseWithLocation.Init<T>
        {
            private List<BinaryValue> keys = new ArrayList<BinaryValue>();
            
            T addKeys(List<BinaryValue> keys)
            {
                this.keys.addAll(keys);
                return self();
            }
            
            T addKey(BinaryValue key) 
            {
                this.keys.add(key);
                return self();
            }
        }
        
        static class Builder extends Init<Builder>
        {

            @Override
            protected Builder self()
            {
                return this;
            }

            @Override
            Response build()
            {
                return new Response(this);
            }
            
        }

    }
}
