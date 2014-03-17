/*
 * Copyright 2013 Basho Technologies Inc.
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
import com.basho.riak.client.query.search.YokozunaIndex;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakYokozunaPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class YzFetchIndexOperation extends FutureOperation<YzFetchIndexOperation.Response, RiakYokozunaPB.RpbYokozunaIndexGetResp>
{    
    private final RiakYokozunaPB.RpbYokozunaIndexGetReq.Builder reqBuilder;
    private final String indexName;
    
    private YzFetchIndexOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
        this.indexName = builder.indexName;
    }
    
    @Override
    protected Response convert(List<RiakYokozunaPB.RpbYokozunaIndexGetResp> rawResponse) throws ExecutionException
    {
        // This isn't a streaming op, so there's only one protobuf in the list
        RiakYokozunaPB.RpbYokozunaIndexGetResp response = rawResponse.get(0);
        List<YokozunaIndex> indexList = new ArrayList<YokozunaIndex>(response.getIndexCount());
        
        for (RiakYokozunaPB.RpbYokozunaIndex pbIndex : response.getIndexList())
        {
            indexList.add(new YokozunaIndex(pbIndex.getName().toStringUtf8(),
                                            pbIndex.getSchema().toStringUtf8()));
        }
        
        return new Response(indexName, indexList);
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakYokozunaPB.RpbYokozunaIndexGetReq req = reqBuilder.build();
        return new RiakMessage(RiakMessageCodes.MSG_GetYzIndexReq, req.toByteArray());
        
    }

    @Override
    protected RiakYokozunaPB.RpbYokozunaIndexGetResp decode(RiakMessage rawMessage)
    {
        Operations.checkMessageType(rawMessage, RiakMessageCodes.MSG_GetYzIndexResp);
        try
        {
            return RiakYokozunaPB.RpbYokozunaIndexGetResp.parseFrom(rawMessage.getData());
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new IllegalArgumentException("Invalid message received", ex);
        }
        
    }
    
    public static class Builder
    {
        private final RiakYokozunaPB.RpbYokozunaIndexGetReq.Builder reqBuilder =  
            RiakYokozunaPB.RpbYokozunaIndexGetReq.newBuilder();
        private String indexName = "All Indexes";
        
        public Builder()
        {}
        
        public Builder withIndexName(String indexName) 
        {
            if (null == indexName || indexName.length() == 0)
            {
                throw new IllegalArgumentException("Index name cannot be null or zero length");
            }
            reqBuilder.setName(ByteString.copyFromUtf8(indexName));
            this.indexName = indexName;
            return this;
        }
        
        public YzFetchIndexOperation build()
        {
            return new YzFetchIndexOperation(this);
        }
    }
    
    public static class Response
    {
        private final List<YokozunaIndex> indexList;
        private final String indexName;
        
        Response(String indexName, List<YokozunaIndex> indexList)
        {
            this.indexName = indexName;
            this.indexList = indexList;
        }
        
        public String getIndexName()
        {
            return indexName;
        }
        
        public List<YokozunaIndex> getIndexes()
        {
            return this.indexList;
        }
    }
}
