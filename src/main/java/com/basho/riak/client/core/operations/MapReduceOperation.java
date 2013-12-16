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
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * A Map/Reduce Operation on Riak. No error checking is done on the content type of the content itself
 * with the exception to making sure they are provided.
 */
public class MapReduceOperation extends FutureOperation<List<ByteArrayWrapper>, RiakKvPB.RpbMapRedResp>
{
    private final RiakKvPB.RpbMapRedReq.Builder reqBuilder;
    
    private MapReduceOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
    }

    @Override
    protected List<ByteArrayWrapper> convert(List<RiakKvPB.RpbMapRedResp> rawResponse) throws ExecutionException
    {
        List<ByteArrayWrapper> results = new ArrayList<ByteArrayWrapper>(rawResponse.size());
        for (RiakKvPB.RpbMapRedResp response : rawResponse)
        {
            if (response.hasResponse())
            {
                results.add(ByteArrayWrapper.create(response.getResponse().toByteArray()));
            }
        }
        return results;
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakKvPB.RpbMapRedReq request = reqBuilder.build();
        return new RiakMessage(RiakMessageCodes.MSG_MapRedReq, request.toByteArray());
    }

    @Override
    protected RiakKvPB.RpbMapRedResp decode(RiakMessage rawMessage)
    {
        Operations.checkMessageType(rawMessage, RiakMessageCodes.MSG_MapRedResp);
        try
        {
            return RiakKvPB.RpbMapRedResp.parseFrom(rawMessage.getData());
        }
        catch (InvalidProtocolBufferException e)
        {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    protected boolean done(RiakKvPB.RpbMapRedResp message)
    {
        return message.getDone();
    }
    
    public static class Builder
    {
        private final RiakKvPB.RpbMapRedReq.Builder reqBuilder =
            RiakKvPB.RpbMapRedReq.newBuilder();
        
        /**
     * Create a MapReduce operation builder with the given function.
     *
     * @param function    a binary blob of type {@code contentType}
     * @param contentType a http-style content encoding type (typically application/json)
     */
        public Builder(ByteArrayWrapper function, String contentType)
        {

            if ((null == function) || function.length() == 0)
            {
                throw new IllegalArgumentException("Function can not be null or empty");
            }

            if ((null == contentType) || contentType.length() == 0)
            {
                throw new IllegalArgumentException("contentType can not be null or empty");
            }
            
            reqBuilder.setRequest(ByteString.copyFrom(function.unsafeGetValue()))
                        .setContentType(ByteString.copyFromUtf8(contentType));
        
        }
        
        public MapReduceOperation build()
        {
            return new MapReduceOperation(this);
        }
    }
    
}
