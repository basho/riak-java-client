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
import com.basho.riak.client.query.search.YokozunaSchema;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakYokozunaPB;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class YzPutSchemaOperation extends FutureOperation<Void, Void>
{
    private final YokozunaSchema schema;
    
    public YzPutSchemaOperation(YokozunaSchema schema)
    {
        if (null == schema)
        {
            throw new IllegalArgumentException("Schema can not be null");
        }
        this.schema = schema;
    }
    
    @Override
    protected Void convert(List<Void> rawResponse) throws ExecutionException
    {
        return null;
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakYokozunaPB.RpbYokozunaSchema.Builder schemaBuilder = 
            RiakYokozunaPB.RpbYokozunaSchema.newBuilder();
        
        schemaBuilder.setName(ByteString.copyFromUtf8(schema.getName()));
        schemaBuilder.setContent(ByteString.copyFromUtf8(schema.getContent()));
        
        RiakYokozunaPB.RpbYokozunaSchemaPutReq.Builder builder =
            RiakYokozunaPB.RpbYokozunaSchemaPutReq.newBuilder();
        
        builder.setSchema(schemaBuilder);
        RiakYokozunaPB.RpbYokozunaSchemaPutReq req = builder.build();
        return new RiakMessage(RiakMessageCodes.MSG_PutYzSchemaReq, req.toByteArray());
    }

    @Override
    protected Void decode(RiakMessage rawMessage)
    {
        byte pbMessageCode = rawMessage.getCode();
        
        if (RiakMessageCodes.MSG_PutResp != pbMessageCode)
        {
            throw new IllegalArgumentException("Wrong response; expected "
                + RiakMessageCodes.MSG_PutResp
                + " received " + pbMessageCode, null);
        }
        
        return null;
    }
}
