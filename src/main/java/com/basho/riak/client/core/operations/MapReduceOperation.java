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
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Map/Reduce Operation on Riak. No error checking is done on the content type of the content itself
 * with the exception to making sure they are provided.
 */
public class MapReduceOperation extends FutureOperation<MapReduceOperation.Response, RiakKvPB.RpbMapRedResp, BinaryValue>
{
    private final RiakKvPB.RpbMapRedReq.Builder reqBuilder;
    private final BinaryValue mapReduce;
    private final Logger logger = LoggerFactory.getLogger(MapReduceOperation.class);

    private MapReduceOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
        this.mapReduce = builder.mapReduce;
    }

    @Override
    protected Response convert(List<RiakKvPB.RpbMapRedResp> rawResponse)
    {
        // Riak streams the result back. Each message from Riak contains a int
        // that tells you what phase the result is from. The result from a phase
        // can span multiple messages. Each result chunk is a JSON array.

        final JsonNodeFactory factory = JsonNodeFactory.instance;
        final ObjectMapper mapper = new ObjectMapper();
        final Map<Integer, ArrayNode> resultMap = new LinkedHashMap<>();

        int phase = 0;

        for (RiakKvPB.RpbMapRedResp response : rawResponse)
        {
            if (response.hasPhase())
            {
                phase = response.getPhase();
            }
            if (response.hasResponse())
            {
                ArrayNode jsonArray;
                if (resultMap.containsKey(phase))
                {
                    jsonArray = resultMap.get(phase);
                }
                else
                {
                    jsonArray = factory.arrayNode();
                    resultMap.put(phase, jsonArray);
                }

                JsonNode responseJson;
                try
                {
                    responseJson = mapper.readTree(response.getResponse().toStringUtf8());
                }
                catch (IOException ex)
                {
                    logger.error("Mapreduce job returned non-JSON; {}",response.getResponse().toStringUtf8());
                    throw new RuntimeException("Non-JSON response from MR job", ex);
                }

                if (responseJson.isArray())
                {
                    jsonArray.addAll((ArrayNode)responseJson);
                }
                else
                {
                    logger.error("Mapreduce job returned JSON that wasn't an array; {}", response.getResponse().toStringUtf8());
                }
            }
        }
        return new Response(resultMap);
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
        Operations.checkPBMessageType(rawMessage, RiakMessageCodes.MSG_MapRedResp);
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
         */
        public Builder(BinaryValue mapReduce)
        {
            if ((null == mapReduce) || mapReduce.length() == 0)
            {
                throw new IllegalArgumentException("MapReduce can not be null or empty.");
            }

            reqBuilder.setRequest(ByteString.copyFrom(mapReduce.unsafeGetValue()))
                        .setContentType(ByteString.copyFromUtf8("application/json"));
            this.mapReduce = mapReduce;
        }

        public MapReduceOperation build()
        {
            return new MapReduceOperation(this);
        }
    }

    public static class Response
    {
        private final Map<Integer, ArrayNode> resultMap;

        Response(Map<Integer, ArrayNode> results)
        {
            this.resultMap = results;
        }

        public Map<Integer, ArrayNode> getResults()
        {
            return resultMap;
        }
    }
}
