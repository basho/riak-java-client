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
import com.basho.riak.client.query.indexes.RiakIndexes;
import com.basho.riak.client.query.links.RiakLink;
import com.basho.riak.client.query.links.RiakLinks;
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakPB.RpbPair;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class GetRespConverter implements RiakResponseConverter<List<RiakObject>>
{

    private final ByteArrayWrapper key;
    private final ByteArrayWrapper bucket;
    private final boolean isHeadRequest;

    public GetRespConverter(ByteArrayWrapper bucket, ByteArrayWrapper key, boolean isHeadRequest)
    {
        this.bucket = bucket;
        this.key = key;
        this.isHeadRequest = isHeadRequest;
    }

    @Override
    public List<RiakObject> convert(RiakKvPB.RpbGetResp resp) throws ExecutionException
    {


        if (null == resp)
        {
            List<RiakObject> objectList = new ArrayList<RiakObject>(1);
            objectList.add(RiakObject.create(bucket.unsafeGetValue())
                .setKey(key.unsafeGetValue())
                .setNotFound(true)
                .setModified(false));
            return objectList;
        }


        int count = resp.getContentCount();

        // To unify the behavior of having just a tombstone vs. siblings
        // that include a tombstone, we create an empty object and mark
        // it deleted
        if (count == 0)
        {
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

            RiakObject riakObject =
                RiakObject.create(bucket.unsafeGetValue())
                    .setKey(key.unsafeGetValue())
                    .unsafeSetValue(content.getValue().toByteArray())
                    .setVClock(vclock.toByteArray())
                    .setContentType(nullSafeByteStringToUtf8(content.getContentType()))
                    .setCharset(nullSafeByteStringToUtf8(content.getCharset()))
                    .setVTag(nullSafeByteStringToUtf8(content.getVtag()))
                    .setDeleted(content.getDeleted())
                    .setModified(!resp.getUnchanged());

            if (content.getLinksCount() > 0)
            {
                riakObject.setLinks(decodeLinks(content));
            }

            if (content.hasLastMod())
            {
                int lastMod = content.getLastMod();
                int lastModUsec = content.getLastModUsecs();
                riakObject.setLastModified((lastMod * 1000L) + (lastModUsec / 1000L));
            }

            if (content.getUsermetaCount() > 0)
            {
                RiakUserMetadata userMeta = new RiakUserMetadata();
                for (int j = 0; j < content.getUsermetaCount(); j++)
                {
                    RpbPair pair = content.getUsermeta(j);
                    userMeta.put(ByteArrayWrapper.unsafeCreate(pair.getKey().toByteArray()),
                        ByteArrayWrapper.unsafeCreate(pair.getValue().toByteArray()));
                }
                riakObject.setUserMeta(userMeta);
            }

            if (content.getIndexesCount() > 0)
            {
                RiakIndexes indexes = new RiakIndexes();
                for (RpbPair p : content.getIndexesList())
                {
                    String name = p.getKey().toStringUtf8();
                    try
                    {
                        IndexType type = IndexType.typeFromFullname(name);
                        indexes.getIndex(new RawIndex.Name(name, type))
                            .add(ByteArrayWrapper.unsafeCreate(p.getValue().toByteArray()));
                    }
                    catch (IllegalArgumentException e)
                    {
                        throw new ExecutionException("Unknown index type" + name, e);
                    }
                }
                riakObject.setIndexes(indexes);
            }

            objectList.add(riakObject);
        }

        return objectList;

    }

    private RiakLinks decodeLinks(RiakKvPB.RpbContent content)
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

        return new RiakLinks().addLinks(decodedLinkList);

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
