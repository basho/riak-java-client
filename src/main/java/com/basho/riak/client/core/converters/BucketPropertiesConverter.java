/*
 * Copyright 2013 Brian Roach <roach at basho dot com>.
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

package com.basho.riak.client.core.converters;

import com.basho.riak.client.core.operations.Operations;
import com.basho.riak.client.core.query.BucketProperties;
import com.basho.riak.client.core.query.functions.Function;
import com.basho.riak.protobuf.RiakPB;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class BucketPropertiesConverter
{
    private BucketPropertiesConverter() {}
    
    public static BucketProperties convert(RiakPB.RpbBucketProps pbProps)
    {
        BucketProperties.Builder builder = new BucketProperties.Builder()
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
            .withLegacyRiakSearchEnabled(pbProps.getSearch())
            .withChashkeyFunction(
                new Function.Builder()
                    .withModule(pbProps.getChashKeyfun().getModule().toStringUtf8())
                    .withFunction(pbProps.getChashKeyfun().getFunction().toStringUtf8())
                    .build());
            
            if (pbProps.hasLinkfun())
            {
                builder.withLinkwalkFunction(
                new Function.Builder()
                    .withModule(pbProps.getLinkfun().getModule().toStringUtf8())
                    .withFunction(pbProps.getLinkfun().getFunction().toStringUtf8())
                    .build());
            }
        
            if (pbProps.hasHasPrecommit())
            {
                for (Function f : parseHooks(pbProps.getPrecommitList()))
                {
                    builder.withPrecommitHook(f);
                }
            }
            
            if (pbProps.hasHasPostcommit())
            {
                for (Function f : parseHooks(pbProps.getPostcommitList()))
                {
                    builder.withPostcommitHook(f);
                }
            }
            
            if (pbProps.hasSearchIndex())
            {
                builder.withSearchIndex(pbProps.getSearchIndex().toStringUtf8());
            }
            
            if (pbProps.hasBackend())
            {
                builder.withBackend(pbProps.getBackend().toStringUtf8());
            }
            
            return builder.build();
    }
    
    public static RiakPB.RpbBucketProps convert(BucketProperties bucketProperties)
    {
        RiakPB.RpbBucketProps.Builder propsBuilder =
            RiakPB.RpbBucketProps.newBuilder();
        
        if (bucketProperties.hasNVal())
        {
            propsBuilder.setNVal(bucketProperties.getNVal());
        }
        if (bucketProperties.hasAllowMulti())
        {
            propsBuilder.setAllowMult(bucketProperties.getAllowMulti());
        }
        if (bucketProperties.hasLastWriteWins())
        {
            propsBuilder.setLastWriteWins(bucketProperties.getLastWriteWins());
        }
        if (bucketProperties.hasPrecommitHooks())
        {
            propsBuilder.addAllPrecommit(convertHooks(bucketProperties.getPrecommitHooks()));
        }
        if (bucketProperties.hasPostcommitHooks())
        {
            propsBuilder.addAllPostcommit(convertHooks(bucketProperties.getPostcommitHooks()));
        }
        if (bucketProperties.hasChashKeyFunction())
        {
            propsBuilder.setChashKeyfun(convertModFun(bucketProperties.getChashKeyFunction()));
        }
        if (bucketProperties.hasLinkwalkFunction())
        {
            propsBuilder.setLinkfun(convertModFun(bucketProperties.getLinkwalkFunction()));
        }
        if (bucketProperties.hasOldVClock())
        {
            propsBuilder.setOldVclock(bucketProperties.getOldVClock().intValue());
        }
        if (bucketProperties.hasYoungVClock())
        {
            propsBuilder.setYoungVclock(bucketProperties.getYoungVClock().intValue());
        }
        if (bucketProperties.hasBigVClock())
        {
            propsBuilder.setBigVclock(bucketProperties.getBigVClock().intValue());
        }
        if (bucketProperties.hasSmallVClock())
        {
            propsBuilder.setSmallVclock(bucketProperties.getSmallVClock().intValue());
        }
        if (bucketProperties.hasPr())
        {
            propsBuilder.setPr(bucketProperties.getPr().getIntValue());
        }
        if (bucketProperties.hasR())
        {
            propsBuilder.setR(bucketProperties.getR().getIntValue());
        }
        if (bucketProperties.hasW())
        {
            propsBuilder.setW(bucketProperties.getW().getIntValue());
        }
        if (bucketProperties.hasPw())
        {
            propsBuilder.setPw(bucketProperties.getPw().getIntValue());
        }
        if (bucketProperties.hasDw())
        {
            propsBuilder.setDw(bucketProperties.getDw().getIntValue());
        }
        if (bucketProperties.hasRw())
        {
            propsBuilder.setRw(bucketProperties.getRw().getIntValue());
        }
        if (bucketProperties.hasBasicQuorum())
        {
            propsBuilder.setBasicQuorum(bucketProperties.getBasicQuorum());
        }
        if (bucketProperties.hasNotFoundOk())
        {
            propsBuilder.setNotfoundOk(bucketProperties.getNotFoundOk());
        }
        if (bucketProperties.hasBackend())
        {
            propsBuilder.setBackend(ByteString.copyFromUtf8(bucketProperties.getBackend()));
        }
        if (bucketProperties.hasLegacyRiakSearchEnabled())
        {
            propsBuilder.setSearch(bucketProperties.getLegacyRiakSearchEnabled());
        }
        if (bucketProperties.hasSearchIndex())
        {
            propsBuilder.setSearchIndex(ByteString.copyFromUtf8(bucketProperties.getSearchIndex()));
        }
        
        return propsBuilder.build();
        
    }
    
    
    private static List<Function> parseHooks(List<RiakPB.RpbCommitHook> hooks)
    {
        List<Function> list = new ArrayList<Function>(hooks.size());
        for ( RiakPB.RpbCommitHook hook : hooks)
        {
            if (hook.hasName())
            {
                Function f = 
                    new Function.Builder()
                        .withName(hook.getName().toStringUtf8())
                        .build();
                list.add(f);
            }
            else
            {
                Function f = new Function.Builder()
                    .withModule(hook.getModfun().getModule().toStringUtf8())
                    .withFunction(hook.getModfun().getFunction().toStringUtf8())
                    .build();
                list.add(f);
            }
        }
        return list;
    }
    
    private static RiakPB.RpbModFun convertModFun(Function f)
    {
        return RiakPB.RpbModFun.newBuilder()
                    .setModule(ByteString.copyFromUtf8(f.getModule()))
                    .setFunction(ByteString.copyFromUtf8(f.getFunction()))
                    .build();
    }
    
    private static List<RiakPB.RpbCommitHook> convertHooks(List<Function> hookList)
    {
        List<RiakPB.RpbCommitHook> pbHookList = new ArrayList<RiakPB.RpbCommitHook>(hookList.size());
        RiakPB.RpbCommitHook.Builder builder = RiakPB.RpbCommitHook.newBuilder();
        RiakPB.RpbModFun.Builder mfBuilder = RiakPB.RpbModFun.newBuilder();
        for (Function hook : hookList)
        {
            if (hook.isJavascript())
            {
                builder.setName(ByteString.copyFromUtf8(hook.getName()));
            }
            else
            {
                mfBuilder.setModule(ByteString.copyFromUtf8(hook.getModule()));
                mfBuilder.setFunction(ByteString.copyFromUtf8(hook.getFunction()));
                builder.setModfun(mfBuilder);
            }
            
            pbHookList.add(builder.build());
            builder.clear();
            mfBuilder.clear();
        }
        return pbHookList;
    }
}
