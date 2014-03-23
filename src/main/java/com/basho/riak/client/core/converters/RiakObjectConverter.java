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

import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.query.UserMetadata.RiakUserMetadata;
import com.basho.riak.client.query.indexes.IndexType;
import com.basho.riak.client.query.indexes.RawIndex;
import com.basho.riak.client.query.indexes.RiakIndex;
import com.basho.riak.client.query.indexes.RiakIndexes;
import com.basho.riak.client.query.links.RiakLink;
import com.basho.riak.client.query.links.RiakLinks;
import com.basho.riak.client.util.BinaryValue;
import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakKvPB.RpbContent;
import com.basho.riak.protobuf.RiakPB;
import com.google.protobuf.ByteString;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for converting to/from RiakKvPB.RpbContent and RiakObject
 * 
 * 
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class RiakObjectConverter
{
    private final static Logger logger = LoggerFactory.getLogger(RiakObjectConverter.class);
    
    private RiakObjectConverter() {}
    
    public static List<RiakObject> convert(List<RpbContent> contentList)
    {
        List<RiakObject> roList = new LinkedList<RiakObject>();
        for (RpbContent content : contentList)
        {
            RiakObject ro = new RiakObject();
                    
            if (content.hasDeleted())
            {
                ro.setDeleted(content.getDeleted());
            }
            
            if (content.hasContentType())
            {
                ro.setContentType(content.getContentType().toStringUtf8());
            }
            
            if (content.hasCharset())
            {
                ro.setCharset(content.getCharset().toStringUtf8());
            }
            
            if (content.hasLastMod())
            {
                int lastMod = content.getLastMod();
                int lastModUsec = content.getLastModUsecs();
                ro.setLastModified((lastMod * 1000L) + (lastModUsec / 1000L));
            }
            
            if (content.hasValue() && !content.getValue().isEmpty())
            {
                ro.setValue(BinaryValue.unsafeCreate(content.getValue().toByteArray()));
            }
            
            if (content.hasVtag())
            {
                ro.setVTag(content.getVtag().toStringUtf8());
            }
            
            if (content.getLinksCount() > 0)
            {
                List<RiakKvPB.RpbLink> pbLinkList = content.getLinksList();
                RiakLinks riakLinks = ro.getLinks();
                
                for (RiakKvPB.RpbLink pbLink : pbLinkList)
                {
                    RiakLink link = new RiakLink(pbLink.getBucket().toStringUtf8(),
                        pbLink.getKey().toStringUtf8(),
                        pbLink.getTag().toStringUtf8());
                    riakLinks.addLink(link);
                }
            }
            
            if (content.getIndexesCount() > 0)
            {
                RiakIndexes indexes = ro.getIndexes();
                for (RiakPB.RpbPair p : content.getIndexesList())
                {
                    String name = p.getKey().toStringUtf8();
                    try
                    {
                        IndexType type = IndexType.typeFromFullname(name);
                        indexes.getIndex(RawIndex.named(name, type))
                            .add(BinaryValue.unsafeCreate(p.getValue().toByteArray()));
                    }
                    catch (IllegalArgumentException e)
                    {
                        logger.error("Unknown index type during conversion: {};{}", name, e);
                    }
                }
            }
            
            if (content.getUsermetaCount() > 0)
            {
                RiakUserMetadata userMeta = ro.getUserMeta();
                for (int j = 0; j < content.getUsermetaCount(); j++)
                {
                    RiakPB.RpbPair pair = content.getUsermeta(j);
                    userMeta.put(BinaryValue.unsafeCreate(pair.getKey().toByteArray()),
                        BinaryValue.unsafeCreate(pair.getValue().toByteArray()));
                }
            }
             
            roList.add(ro);
        }
        
        return roList;
    }
    
    public static RpbContent convert(RiakObject ro)
    {
        RpbContent.Builder builder = RpbContent.newBuilder();
        
        builder.setContentType(ByteString.copyFromUtf8(ro.getContentType()));
        
        if (ro.hasCharset())
        {
            builder.setCharset(ByteString.copyFromUtf8(ro.getCharset()));
        }
        
        if (ro.hasValue())
        {
            builder.setValue(ByteString.copyFrom(ro.getValue().unsafeGetValue()));
        }
        
        if (ro.hasLinks())
        {
            for (RiakLink link : ro.getLinks())
            {
                builder.addLinks(
                    RiakKvPB.RpbLink.newBuilder()
                        .setBucket(ByteString.copyFrom(link.getBucketAsBytes().unsafeGetValue()))
                        .setTag(ByteString.copyFrom(link.getTagAsBytes().unsafeGetValue()))
                        .setKey(ByteString.copyFrom(link.getKeyAsBytes().unsafeGetValue())));
            }
        }
         
        if (ro.hasIndexes())
        {
            for (RiakIndex<?> index : ro.getIndexes())
            {
                for (BinaryValue value : index.rawValues())
                {
                    RiakPB.RpbPair.Builder pair = RiakPB.RpbPair.newBuilder();
                    pair.setKey(ByteString.copyFrom(index.getFullname().getBytes()));
                    pair.setValue(ByteString.copyFrom(value.unsafeGetValue()));
                    builder.addIndexes(pair);
                }
            }
        }
        
        if (ro.hasUserMeta())
        {
            for (Map.Entry<BinaryValue,BinaryValue> entry 
                 : ro.getUserMeta().getUserMetadata())
            {
                RiakPB.RpbPair.Builder pair = RiakPB.RpbPair.newBuilder();
                pair.setKey(ByteString.copyFrom(entry.getKey().unsafeGetValue()));
                pair.setValue(ByteString.copyFrom(entry.getValue().unsafeGetValue()));
                builder.addUsermeta(pair);
            }
        }
        
        return builder.build();
    }
}
