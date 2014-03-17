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
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakYokozunaPB;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class YzDeleteIndexOperation extends FutureOperation<YzDeleteIndexOperation.Response, Void>
{
    private final RiakYokozunaPB.RpbYokozunaIndexDeleteReq.Builder reqBuilder;
    private final String indexName;
    
    private YzDeleteIndexOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
        this.indexName = builder.indexName;
    }
    
    @Override
    protected YzDeleteIndexOperation.Response convert(List<Void> rawResponse) throws ExecutionException
    {
        return new Response(indexName);
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakYokozunaPB.RpbYokozunaIndexDeleteReq req = reqBuilder.build();
        return new RiakMessage(RiakMessageCodes.MSG_DelYzIndexReq, req.toByteArray());
        
    }

    @Override
    protected Void decode(RiakMessage rawMessage)
    {
        Operations.checkMessageType(rawMessage, RiakMessageCodes.MSG_DelResp);
        return null;
    }
    
    public static class Builder
    {
        private RiakYokozunaPB.RpbYokozunaIndexDeleteReq.Builder reqBuilder =
            RiakYokozunaPB.RpbYokozunaIndexDeleteReq.newBuilder();
        private final String indexName;
        
        public Builder(String indexName)
        {
            if (null == indexName || indexName.length() == 0)
            {
                throw new IllegalArgumentException("Index name cannot be null or zero length");
            }
            reqBuilder.setName(ByteString.copyFromUtf8(indexName));
            this.indexName = indexName;
        }
        
        public YzDeleteIndexOperation build()
        {
            return new YzDeleteIndexOperation(this);
        }
    }
    
    public static class Response
    {
        private final String indexName;
        
        Response(String indexName)
        {
            this.indexName = indexName;
        }
        
        public String getIndexName()
        {
            return indexName;
        }
    }
    
}
