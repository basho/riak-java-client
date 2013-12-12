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
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class YzPutIndexOperation extends FutureOperation<Boolean, Void>
{
    private final RiakYokozunaPB.RpbYokozunaIndexPutReq.Builder reqBuilder;
    
    private YzPutIndexOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
    }
    
    @Override
    protected Boolean convert(List<Void> rawResponse) throws ExecutionException
    {
        return true;
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakYokozunaPB.RpbYokozunaIndexPutReq req = reqBuilder.build();
        return new RiakMessage(RiakMessageCodes.MSG_PutYzIndexReq, req.toByteArray());
        
    }

    @Override
    protected Void decode(RiakMessage rawMessage)
    {
        Operations.checkMessageType(rawMessage, RiakMessageCodes.MSG_PutResp);
        return null;
    }
    
    public static class Builder
    {
        private final RiakYokozunaPB.RpbYokozunaIndexPutReq.Builder reqBuilder =
            RiakYokozunaPB.RpbYokozunaIndexPutReq.newBuilder();
        
        public Builder(YokozunaIndex index)
        {
            if (null == index)
            {
                throw new IllegalArgumentException("Index can not be null");
            }
            
            RiakYokozunaPB.RpbYokozunaIndex.Builder indexBuilder =
                RiakYokozunaPB.RpbYokozunaIndex.newBuilder();
        
            indexBuilder.setName(ByteString.copyFromUtf8(index.getName()));
            // A null schema is valid; the default will be used 
            if (index.getSchema() != null)
            {
                indexBuilder.setSchema(ByteString.copyFromUtf8(index.getSchema()));
            }
            
            reqBuilder.setIndex(indexBuilder);
        }
        
        public YzPutIndexOperation build()
        {
            return new YzPutIndexOperation(this);
        }
        
    }
}
