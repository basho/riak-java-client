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
import com.basho.riak.client.core.query.search.YokozunaSchema;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakYokozunaPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class YzGetSchemaOperation extends FutureOperation<YzGetSchemaOperation.Response, RiakYokozunaPB.RpbYokozunaSchemaGetResp, String>
{
    private final RiakYokozunaPB.RpbYokozunaSchemaGetReq.Builder reqBuilder;
    private final String schemaName;

    private YzGetSchemaOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
        this.schemaName = builder.schemaName;
    }
    
    @Override
    protected YzGetSchemaOperation.Response convert(List<RiakYokozunaPB.RpbYokozunaSchemaGetResp> rawResponse)
    {
        // This isn't a streaming op, so there's only one protobuf in the list
        RiakYokozunaPB.RpbYokozunaSchemaGetResp response = rawResponse.get(0);
        
        return new Response(new YokozunaSchema(response.getSchema().getName().toStringUtf8(),
                                    response.getSchema().getContent().toStringUtf8()));
        
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakYokozunaPB.RpbYokozunaSchemaGetReq req = reqBuilder.build();
        return new RiakMessage(RiakMessageCodes.MSG_YokozunaSchemaGetReq, req.toByteArray());
        
    }

    @Override
    protected RiakYokozunaPB.RpbYokozunaSchemaGetResp decode(RiakMessage rawMessage)
    {
        Operations.checkMessageType(rawMessage, RiakMessageCodes.MSG_YokozunaSchemaGetResp);
        try
        {
            return RiakYokozunaPB.RpbYokozunaSchemaGetResp.parseFrom(rawMessage.getData());
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new IllegalArgumentException("Invalid message received", ex);
        }
        
    }

    @Override
    public String getQueryInfo()
    {
        return schemaName;
    }
    
    public static class Builder
    {
        private final RiakYokozunaPB.RpbYokozunaSchemaGetReq.Builder reqBuilder = 
            RiakYokozunaPB.RpbYokozunaSchemaGetReq.newBuilder();
        private final String schemaName;
        
        public Builder(String schemaName)
        {
            if (null == schemaName || schemaName.length() == 0)
            {
                throw new IllegalArgumentException("Schema name cannot be null or zero length");
            }
            reqBuilder.setName(ByteString.copyFromUtf8(schemaName));
            this.schemaName = schemaName;
        }
        
        public YzGetSchemaOperation build()
        {
            return new YzGetSchemaOperation(this);
        }
    }
    
    public static class Response
    {
        private final YokozunaSchema schema;
        
        Response(YokozunaSchema schema)
        {
            this.schema = schema;
        }
        
        public YokozunaSchema getSchema()
        {
            return schema;
        }
    }
}
