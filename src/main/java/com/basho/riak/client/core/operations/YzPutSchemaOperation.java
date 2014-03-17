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
public class YzPutSchemaOperation extends FutureOperation<YzPutSchemaOperation.Response, Void>
{
    private final RiakYokozunaPB.RpbYokozunaSchemaPutReq.Builder reqBuilder;
    private final YokozunaSchema schema;
    
    private YzPutSchemaOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
        this.schema = builder.schema;
    }
    
    @Override
    protected YzPutSchemaOperation.Response convert(List<Void> rawResponse) throws ExecutionException
    {
        return new Response(schema);
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakYokozunaPB.RpbYokozunaSchemaPutReq req = reqBuilder.build();
        return new RiakMessage(RiakMessageCodes.MSG_PutYzSchemaReq, req.toByteArray());
    }

    @Override
    protected Void decode(RiakMessage rawMessage)
    {
        Operations.checkMessageType(rawMessage, RiakMessageCodes.MSG_PutResp);
        return null;
    }
    
    public static class Builder
    {
        private final RiakYokozunaPB.RpbYokozunaSchemaPutReq.Builder reqBuilder =
            RiakYokozunaPB.RpbYokozunaSchemaPutReq.newBuilder();
        private final YokozunaSchema schema;
        
        public Builder(YokozunaSchema schema)
        {
            RiakYokozunaPB.RpbYokozunaSchema.Builder schemaBuilder = 
            RiakYokozunaPB.RpbYokozunaSchema.newBuilder();
        
            schemaBuilder.setName(ByteString.copyFromUtf8(schema.getName()));
            schemaBuilder.setContent(ByteString.copyFromUtf8(schema.getContent()));
            reqBuilder.setSchema(schemaBuilder);
            this.schema = schema;
        }
        
        public YzPutSchemaOperation build()
        {
            return new YzPutSchemaOperation(this);
        }
    }
    
    public static class Response
    {
        private final YokozunaSchema schema;
        
        Response(YokozunaSchema schema)
        {
            this.schema = schema;
        }
        
        public YokozunaSchema getScema()
        {
            return schema;
        }
    }
}
