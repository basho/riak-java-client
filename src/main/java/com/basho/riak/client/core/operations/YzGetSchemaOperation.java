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
import com.basho.riak.client.query.YokozunaSchema;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakYokozunaPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class YzGetSchemaOperation extends FutureOperation<YokozunaSchema, RiakYokozunaPB.RpbYokozunaSchemaGetResp>
{
    private final String schemaName;

    public YzGetSchemaOperation(String schemaName)
    {
        if (null == schemaName || schemaName.length() == 0)
        {
            throw new IllegalArgumentException("Schema name cannot be null or zero length");
        }
        
        this.schemaName = schemaName;
    }
    
    @Override
    protected YokozunaSchema convert(List<RiakYokozunaPB.RpbYokozunaSchemaGetResp> rawResponse) throws ExecutionException
    {
        // This isn't a streaming op, so there's only one protobuf in the list
        RiakYokozunaPB.RpbYokozunaSchemaGetResp response = rawResponse.get(0);
        
        return new YokozunaSchema(response.getSchema().getName().toStringUtf8(),
                                    response.getSchema().getContent().toStringUtf8());
        
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakYokozunaPB.RpbYokozunaSchemaGetReq.Builder builder = 
            RiakYokozunaPB.RpbYokozunaSchemaGetReq.newBuilder();
        
        builder.setName(ByteString.copyFromUtf8(schemaName));
        RiakYokozunaPB.RpbYokozunaSchemaGetReq req = builder.build();
        return new RiakMessage(RiakMessageCodes.MSG_GetYzSchemaReq, req.toByteArray());
        
    }

    @Override
    protected RiakYokozunaPB.RpbYokozunaSchemaGetResp decode(RiakMessage rawMessage)
    {
        byte pbMessageCode = rawMessage.getCode();
        byte[] data = rawMessage.getData();
        if (RiakMessageCodes.MSG_GetYzSchemaResp != pbMessageCode)
        {
            throw new IllegalArgumentException("Wrong response; expected "
                + RiakMessageCodes.MSG_GetYzSchemaResp
                + " received " + pbMessageCode, null);
        }
        try
        {
            return RiakYokozunaPB.RpbYokozunaSchemaGetResp.parseFrom(data);
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new IllegalArgumentException("Invalid message received", ex);
        }
        
    }
    
}
