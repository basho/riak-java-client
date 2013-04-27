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
package com.basho.riak.client.core.converters;

import com.basho.riak.client.DefaultRiakObject;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.cap.BasicVClock;
import com.basho.riak.client.query.indexes.BinIndex;
import com.basho.riak.client.query.indexes.IntIndex;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakPB.RpbPair;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.handler.codec.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */

public class GetRespConverter implements RiakResponseConverter<List<RiakObject>>
{
    private final String key;
    private final String bucket;
    
    public GetRespConverter(String bucket, String key)
    {
        this.bucket = bucket;
        this.key = key;
    }
    
    @Override
    public List<RiakObject> convert(HttpResponse response, byte[] content)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<RiakObject> convert(byte pbMessageCode, byte[] data) throws ExecutionException
    {
        if (RiakMessageCodes.MSG_GetResp != pbMessageCode)
        {
            throw new ExecutionException("Wrong response; expected " + 
                                         RiakMessageCodes.MSG_GetResp +
                                         " received " + pbMessageCode, null);
        }
        
        try
        {
            RiakKvPB.RpbGetResp resp = RiakKvPB.RpbGetResp.parseFrom(data);
            int count = resp.getContentCount();
            
            // To unify the behavior of having just a tombstone vs. siblings
            // that include a tombstone, we create an empty object and mark
            // it deleted
            if (count == 0) {
                RiakKvPB.RpbGetResp.Builder responseBuilder = resp.toBuilder();
                RiakKvPB.RpbContent.Builder contentBuilder = RiakKvPB.RpbContent.getDefaultInstance().toBuilder();
                contentBuilder.setDeleted(true).setValue(ByteString.EMPTY);
                resp = responseBuilder.addContent(contentBuilder.build()).build();
                count = 1;
            }
            
            List<RiakObject> objectList = new ArrayList<RiakObject>(count);
            ByteString vclock = resp.getVclock();
            
            for (int i = 0; i < count; i++)
            {
                RiakKvPB.RpbContent content = resp.getContent(i);
                DefaultRiakObject.Builder builder = 
                    new DefaultRiakObject.Builder()
                        .withKey(key)
                        .withBucket(bucket)
                        .withValue(content.getValue().toByteArray())
                        .withVClock(new BasicVClock(vclock.toByteArray()))
                        .withContentType(nullSafeByteStringToUtf8(content.getContentType()))
                        .withCharset(nullSafeByteStringToUtf8(content.getCharset()))
                        .withVtag(nullSafeByteStringToUtf8(content.getVtag()))
                        .withDeleted(content.getDeleted());
                        
                if (content.getLinksCount() > 0)
                {
                    builder.withLinks(decodeLinks(content));
                }
                        
                if (content.hasLastMod())
                {
                    int lastMod = content.getLastMod();
                    int lastModUsec = content.getLastModUsecs();
                    builder.withLastModified((lastMod * 1000L ) + (lastModUsec / 1000L));
                }
                
                if (content.getUsermetaCount() > 0)
                {
                    Map<String,String> userMeta = new LinkedHashMap<String,String>();
                    for (int j = 0; j < content.getUsermetaCount(); j++)
                    {
                        RpbPair pair = content.getUsermeta(j);
                        userMeta.put(pair.getKey().toStringUtf8(), nullSafeByteStringToUtf8(pair.getValue()));
                    }
                    builder.withUsermeta(userMeta);
                }
                
                if (content.getIndexesCount() > 0)
                {
                    for (RpbPair p : content.getIndexesList())
                    {
                        String name = p.getKey().toStringUtf8();
                        String value = p.getKey().toStringUtf8();
                        
                        if (name.endsWith(BinIndex.SUFFIX))
                        {
                            builder.addIndex(name, value);
                        }
                        else if (name.endsWith(IntIndex.SUFFIX))
                        {
                            builder.addIndex(name, Long.valueOf(value));
                        }
                        else
                        {
                            throw new ExecutionException("Unknown index type" + name, null);
                        }
                    }
                }
            }
            
            return objectList;
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new ExecutionException(ex);
        }
    }
    
    private List<RiakLink> decodeLinks(RiakKvPB.RpbContent content)
    {
        List<RiakKvPB.RpbLink> pbLinkList = content.getLinksList();
        List<RiakLink> decodedLinkList = new ArrayList<RiakLink>(pbLinkList.size());
        
        for (RiakKvPB.RpbLink pbLink : pbLinkList)
        {
            RiakLink link = new RiakLink(pbLink.getBucket().toStringUtf8(),
                                         pbLink.getKey().toStringUtf8(),
                                         pbLink.getTag().toStringUtf8());
            decodedLinkList.add(link);
        }
        
        return decodedLinkList;
        
    }
    
    private String nullSafeByteStringToUtf8(ByteString bs)
    {
        if (null == bs)
        {
            return null;
        }
        else
        {
            return bs.toStringUtf8();
        }
    }
    
}