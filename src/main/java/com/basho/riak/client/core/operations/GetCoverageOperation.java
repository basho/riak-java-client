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
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.*;

/**
 * An operation to get a coverage information from Riak.
 *
 * @author Sergey Galkin <sgalkin at basho dot com>
 */
public class GetCoverageOperation extends FutureOperation<GetCoverageOperation.Response, RiakKvPB.RpbApiEpResp, String> {
    private final RiakKvPB.RpbApiEpReq.Builder reqBuilder;
    private final String queryInfo;

    private GetCoverageOperation(Builder builder){
        this.reqBuilder = builder.reqBuilder;
        queryInfo = builder.queryInfo;
    }

    @Override
    protected Response convert(List<RiakKvPB.RpbApiEpResp> rawResponse) {
        if (rawResponse.size() != 1)
        {
            throw new IllegalStateException("Expecting exactly one response, instead received " + rawResponse.size());
        }

        RiakKvPB.RpbApiEpResp response = rawResponse.iterator().next();
        return new Response(response.getEplistList());
    }

    @Override
    protected RiakMessage createChannelMessage() {
        return new RiakMessage(RiakMessageCodes.MSG_ApiEpReq, reqBuilder.build().toByteArray());
    }

    @Override
    protected RiakKvPB.RpbApiEpResp decode(RiakMessage rawMessage) {
        try
        {
            Operations.checkMessageType(rawMessage, RiakMessageCodes.MSG_ApiEpResp);
            return RiakKvPB.RpbApiEpResp.parseFrom(rawMessage.getData());
        }
        catch (InvalidProtocolBufferException e)
        {
            throw new IllegalArgumentException("Invalid message received", e);
        }
    }

    @Override
    public String getQueryInfo() {
        return queryInfo;
    }

    public static class Builder {
        private final RiakKvPB.RpbApiEpReq.Builder reqBuilder = RiakKvPB.RpbApiEpReq.newBuilder();
        private String queryInfo = "";

        public Builder(){
            reqBuilder.setProto(RiakKvPB.RpbApiProto.valueOf(RiakKvPB.RpbApiProto.pbc_VALUE));
        }

        public Builder(Location location){
            this();

            if (location == null)
            {
                throw new IllegalArgumentException("Location cannot be null");
            }

            withNamespace(location.getNamespace());
            reqBuilder.setKey(ByteString.copyFrom(location.getKey().unsafeGetValue()));
            queryInfo = location.toString();
        }

        public Builder(Namespace namespace){
            withNamespace(namespace);
        }

        private Builder withNamespace(Namespace namespace){
            if (namespace == null)
            {
                throw new IllegalArgumentException("Namespace cannot be null");
            }

            reqBuilder.setBucket(ByteString.copyFrom(namespace.getBucketName().unsafeGetValue()));
            queryInfo = "{bucket: " + namespace.getBucketName() + "}";
            return this;
        }

        public Builder withForcedUpdate(){
            reqBuilder.setForceUpdate(true);
            return this;
        }

        public GetCoverageOperation build()
        {
            return new GetCoverageOperation(this);
        }
    }

    public static class Response
    {
        private final List<Map.Entry<String,Integer>> entryPoints;

        public Response(List<RiakKvPB.RpbApiEp> epList) {
            entryPoints = new ArrayList<Map.Entry<String,Integer>>(epList.size());
            for(RiakKvPB.RpbApiEp ep: epList){
                entryPoints.add(new AbstractMap.SimpleEntry<String, Integer>(ep.getAddr().toStringUtf8(), ep.getPort()));
            }
        }

        public List<Map.Entry<String,Integer>> geEntryPoints() {
            return entryPoints;
        }
    }
}
