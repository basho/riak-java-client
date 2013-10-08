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
import com.basho.riak.client.query.BucketProperties;
import com.basho.riak.client.query.functions.Function;
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class FetchBucketPropsOperation extends FutureOperation<BucketProperties, RiakPB.RpbGetBucketResp>
{

    private ByteArrayWrapper bucketType;
    private final ByteArrayWrapper bucketName;
    
    public FetchBucketPropsOperation(ByteArrayWrapper bucketName)
    {
        
        if (null == bucketName || bucketName.length() == 0)
        {
            throw new IllegalArgumentException("Bucket name can not be null or zero length");
        }
        this.bucketName = bucketName;
    }
    
    /**
     * Set the bucket type.
     * If unset "default" is used. 
     * @param bucketType the bucket type to use
     * @return A reference to this object.
     */
    public FetchBucketPropsOperation withBucketType(ByteArrayWrapper bucketType)
    {
        if (null == bucketType || bucketType.length() == 0)
        {
            throw new IllegalArgumentException("Bucket type can not be null or zero length");
        }
        this.bucketType = bucketType;
        return this;
    }
    
    @Override
    protected BucketProperties convert(List<RiakPB.RpbGetBucketResp> rawResponse) throws ExecutionException
    {
        // This isn't streaming, there will only be one response. 
        RiakPB.RpbBucketProps pbProps = rawResponse.get(0).getProps();
        BucketProperties props = new BucketProperties()
            .withNVal(pbProps.getNVal())
            .withAllowMulti(pbProps.getAllowMult())
            .withLastWriteWins(pbProps.getLastWriteWins())
            .withOldVClock(Operations.getUnsignedIntValue(pbProps.getOldVclock()))
            .withYoungVClock(Operations.getUnsignedIntValue(pbProps.getYoungVclock()))
            .withBigVClock(Operations.getUnsignedIntValue(pbProps.getBigVclock()))
            .withSmallVClock(Operations.getUnsignedIntValue(pbProps.getSmallVclock()))
            .withPr(pbProps.getPr())
            .withR(pbProps.getR())
            .withW(pbProps.getW())
            .withPw(pbProps.getPw())
            .withDw(pbProps.getDw())
            .withRw(pbProps.getRw())
            .withBasicQuorum(pbProps.getBasicQuorum())
            .withNotFoundOk(pbProps.getNotfoundOk())
            .withRiakSearchEnabled(pbProps.getSearch())
            .withLinkwalkFunction(
                new Function.Builder()
                    .withModule(pbProps.getLinkfun().getModule().toStringUtf8())
                    .withFunction(pbProps.getLinkfun().getFunction().toStringUtf8())
                    .build())
            .withChashkeyFunction(
                new Function.Builder()
                    .withModule(pbProps.getChashKeyfun().getModule().toStringUtf8())
                    .withFunction(pbProps.getChashKeyfun().getFunction().toStringUtf8())
                    .build());
            
            
        
            if (pbProps.hasHasPrecommit())
            {
                for (Function f : parseHooks(pbProps.getPrecommitList()))
                {
                    props.withPrecommitHook(f);
                }
            }
            
            if (pbProps.hasHasPostcommit())
            {
                for (Function f : parseHooks(pbProps.getPostcommitList()))
                {
                    props.withPostcommitHook(f);
                }
            }
            
            if (pbProps.hasYzIndex())
            {
                props.withYokozunaIndex(pbProps.getYzIndex().toStringUtf8());
            }
            
            if (pbProps.hasBackend())
            {
                props.withBackend(pbProps.getBackend().toStringUtf8());
            }
            
            return props;
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakPB.RpbGetBucketReq.Builder builder = 
            RiakPB.RpbGetBucketReq.newBuilder();
        
        if (bucketType !=null)
        {
            builder.setType(ByteString.copyFrom(bucketType.unsafeGetValue()));
        }
        builder.setBucket(ByteString.copyFrom(bucketName.unsafeGetValue()));
        RiakPB.RpbGetBucketReq req = builder.build();
        return new RiakMessage(RiakMessageCodes.MSG_GetBucketReq, req.toByteArray());
    }

    @Override
    protected RiakPB.RpbGetBucketResp decode(RiakMessage rawMessage)
    {
        Operations.checkMessageType(rawMessage, RiakMessageCodes.MSG_GetBucketResp);
        try
        {
            return RiakPB.RpbGetBucketResp.parseFrom(rawMessage.getData());
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new IllegalArgumentException("Invalid message received", ex);
        }
    }
    
    private List<Function> parseHooks(List<RiakPB.RpbCommitHook> hooks) {
        List<Function> list = new ArrayList<Function>(hooks.size());
        for ( RiakPB.RpbCommitHook hook : hooks) {
            if (hook.hasName()) {
                Function f = 
                    new Function.Builder()
                        .withName(hook.getName().toStringUtf8())
                        .build();
                list.add(f);
            } else {
                Function f = new Function.Builder()
                    .withModule(hook.getModfun().getModule().toStringUtf8())
                    .withFunction(hook.getModfun().getFunction().toStringUtf8())
                    .build();
                list.add(f);
            }
        }
        return list;
    }
    
}
