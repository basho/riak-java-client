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
import com.basho.riak.client.util.BinaryValue;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.List;

/**
 * A Map/Reduce Operation on Riak. No error checking is done on the content type of the content itself
 * with the exception to making sure they are provided.
 */
public class MapReduceOperation extends FutureOperation<MapReduceOperation.Response, RiakKvPB.RpbMapRedResp, BinaryValue>
{
    private final RiakKvPB.RpbMapRedReq.Builder reqBuilder;
    private final BinaryValue mapReduce;
    
    private MapReduceOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
        this.mapReduce = builder.mapReduce;
    }

    @Override
    protected Response convert(List<RiakKvPB.RpbMapRedResp> rawResponse)
    {
        List<BinaryValue> results = new ArrayList<BinaryValue>(rawResponse.size());
        for (RiakKvPB.RpbMapRedResp response : rawResponse)
        {
            if (response.hasResponse())
            {
                results.add(BinaryValue.create(response.getResponse().toByteArray()));
            }
        }
        return new Response(mapReduce, results);
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

    @Override
    public BinaryValue getQueryInfo()
    {
        return mapReduce;
    }
    
    public static class Builder
    {
        private final RiakKvPB.RpbMapRedReq.Builder reqBuilder =
            RiakKvPB.RpbMapRedReq.newBuilder();
        private final BinaryValue mapReduce;
        
        /**
         * Create a MapReduce operation builder with the given function.
         *
         * @param mapReduce a mapReduce query.
         * @param contentType a http-style content encoding type (typically application/json)
         */
        public Builder(BinaryValue mapReduce, String contentType)
        {
            if ((null == mapReduce) || mapReduce.length() == 0)
            {
                throw new IllegalArgumentException("MapReduce can not be null or empty.");
            }
            else if ((null == contentType) || contentType.length() == 0)
            {
                throw new IllegalArgumentException("ContentType can not be null or empty.");
            }
            
            reqBuilder.setRequest(ByteString.copyFrom(mapReduce.unsafeGetValue()))
                        .setContentType(ByteString.copyFromUtf8(contentType));
            this.mapReduce = mapReduce;
        
        }
        
        public MapReduceOperation build()
        {
            return new MapReduceOperation(this);
        }
    }
    
    public static class Response
    {
        private final BinaryValue mapReduce;
        private final List<BinaryValue> results;
        
        Response(BinaryValue mapReduce, List<BinaryValue> results)
        {
            this.mapReduce = mapReduce;
            this.results = results;
        }
        
        public BinaryValue getMapReduceQuery()
        {
            return mapReduce;
        }
        
        public List<BinaryValue> getResults()
        {
            return results;
        }
        
    }
}
