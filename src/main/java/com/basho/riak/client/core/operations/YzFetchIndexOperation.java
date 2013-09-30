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
public class YzFetchIndexOperation extends FutureOperation<List<YokozunaIndex>, RiakYokozunaPB.RpbYokozunaIndexGetResp>
{
    private final String indexName;
    
    /**
     * Constructs a YzFetchIndexOperation that will return all indexes.
     */
    public YzFetchIndexOperation()
    {
        this(null);
    }
    
    /**
     * Constructs a YzFetchIndexOperation that will return the specified index.
     * @param indexName the name of the index. 
     */
    public YzFetchIndexOperation(String indexName)
    {
        this.indexName = indexName;
    }
    
    
    @Override
    protected List<YokozunaIndex> convert(List<RiakYokozunaPB.RpbYokozunaIndexGetResp> rawResponse) throws ExecutionException
    {
        // This isn't a streaming op, so there's only one protobuf in the list
        RiakYokozunaPB.RpbYokozunaIndexGetResp response = rawResponse.get(0);
        List<YokozunaIndex> indexList = new ArrayList<YokozunaIndex>(response.getIndexCount());
        
        for (RiakYokozunaPB.RpbYokozunaIndex pbIndex : response.getIndexList())
        {
            indexList.add(new YokozunaIndex(pbIndex.getName().toStringUtf8(),
                                            pbIndex.getSchema().toStringUtf8()));
        }
        
        return indexList;
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakYokozunaPB.RpbYokozunaIndexGetReq.Builder builder =  
            RiakYokozunaPB.RpbYokozunaIndexGetReq.newBuilder();
        if (indexName != null)
        {
            builder.setName(ByteString.copyFromUtf8(indexName));
        }
        
        RiakYokozunaPB.RpbYokozunaIndexGetReq req = builder.build();
        return new RiakMessage(RiakMessageCodes.MSG_GetYzIndexReq, req.toByteArray());
        
    }

    @Override
    protected RiakYokozunaPB.RpbYokozunaIndexGetResp decode(RiakMessage rawMessage)
    {
        byte pbMessageCode = rawMessage.getCode();
        byte[] data = rawMessage.getData();
        if (RiakMessageCodes.MSG_GetYzIndexResp != pbMessageCode)
        {
            throw new IllegalArgumentException("Wrong response; expected "
                + RiakMessageCodes.MSG_GetYzIndexResp
                + " received " + pbMessageCode, null);
        }
        try
        {
            return RiakYokozunaPB.RpbYokozunaIndexGetResp.parseFrom(data);
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new IllegalArgumentException("Invalid message received", ex);
        }
        
    }
}
