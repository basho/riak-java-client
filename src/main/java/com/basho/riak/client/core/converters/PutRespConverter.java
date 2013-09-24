/*
 * Copyright 2013 Basho Technologies Inc
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

import com.basho.riak.client.cap.BasicVClock;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.query.UserMetadata.RiakUserMetadata;
import com.basho.riak.client.query.indexes.IndexType;
import com.basho.riak.client.query.indexes.RawIndex;
import com.basho.riak.client.query.indexes.RiakIndexes;
import com.basho.riak.client.query.links.RiakLink;
import com.basho.riak.client.query.links.RiakLinks;
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakPB;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

import static com.basho.riak.protobuf.RiakKvPB.RpbContent;
import static com.basho.riak.protobuf.RiakKvPB.RpbPutResp;

public class PutRespConverter implements RiakResponseConverter<RpbPutResp, List<RiakObject>>
{

    private final ByteArrayWrapper key;
    private final ByteArrayWrapper bucket;

    public PutRespConverter(ByteArrayWrapper bucket, ByteArrayWrapper key)
    {
        this.key = key;
        this.bucket = bucket;
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

    RiakUserMetadata decodeUserMetadata(List<RiakPB.RpbPair> metaPairs)
    {
        RiakUserMetadata userMeta = new RiakUserMetadata();
        for (RiakPB.RpbPair pair : metaPairs)
        {
            ByteArrayWrapper key = ByteArrayWrapper.unsafeCreate(pair.getKey().toByteArray());
            ByteArrayWrapper value = ByteArrayWrapper.unsafeCreate(pair.getValue().toByteArray());
            userMeta.put(key, value);
        }
        return userMeta;
    }

    RiakIndexes decodeIndexes(List<RiakPB.RpbPair> indexPairs)
    {
        RiakIndexes indexes = new RiakIndexes();
        for (RiakPB.RpbPair p : indexPairs)
        {
            String name = p.getKey().toStringUtf8();

            IndexType type = IndexType.typeFromFullname(name);
            indexes.getIndex(new RawIndex.Name(name, type))
                .add(ByteArrayWrapper.unsafeCreate(p.getValue().toByteArray()));

        }
        return indexes;
    }

    RiakLinks decodeLinks(List<RiakKvPB.RpbLink> pbLinks)
    {

        List<RiakLink> links = new ArrayList<RiakLink>();
        for (RiakKvPB.RpbLink pbLink : pbLinks)
        {
            RiakLink link = new RiakLink(pbLink.getBucket().toStringUtf8(),
                pbLink.getKey().toStringUtf8(),
                pbLink.getTag().toStringUtf8());
            links.add(link);
        }
        return new RiakLinks().addLinks(links);
    }

    List<RiakObject> decodeContents(ByteArrayWrapper key, VClock vclock, List<RpbContent> contents)
    {
        List<RiakObject> objectList = new ArrayList<RiakObject>();
        for (RpbContent content : contents)
        {

            RiakObject riakObject =
                RiakObject.create(bucket.unsafeGetValue())
                    .setKey(key.unsafeGetValue())
                    .setVClock(vclock)
                    .setContentType(nullSafeByteStringToUtf8(content.getContentType()))
                    .setCharset(nullSafeByteStringToUtf8(content.getCharset()))
                    .setDeleted(content.getDeleted())
                    .setLinks(decodeLinks(content.getLinksList()))
                    .setUserMeta(decodeUserMetadata(content.getUsermetaList()))
                    .setIndexes(decodeIndexes(content.getIndexesList()));

            if (content.hasValue() && !content.getValue().isEmpty())
            {
                riakObject.unsafeSetValue(content.getValue().toByteArray());
            }

            if (content.hasVtag())
            {
                riakObject.setVTag(nullSafeByteStringToUtf8(content.getVtag()));
            }

            if (content.hasLastMod())
            {
                int lastMod = content.getLastMod();
                int lastModUsec = content.getLastModUsecs();
                riakObject.setLastModified((lastMod * 1000L) + (lastModUsec / 1000L));
            }

            objectList.add(riakObject);
        }

        return objectList;
    }

    @Override
    public List<RiakObject> convert(RpbPutResp resp)
    {

        // To unify the behavior of having just a tombstone vs. siblings
        // that include a tombstone, we create an empty object and mark
        // it deleted
        if (resp.getContentCount() == 0)
        {
            RpbPutResp.Builder responseBuilder =
                resp.toBuilder()
                    .addContent(
                        RpbContent.newBuilder()
                            .setDeleted(true)
                            .setValue(ByteString.EMPTY));

            resp = responseBuilder.build();
        }

        VClock vClock = new BasicVClock(resp.getVclock().toByteArray());
        ByteArrayWrapper actualKey = resp.hasKey() ? ByteArrayWrapper.create(resp.getKey().toByteArray()) : key;
        return decodeContents(actualKey, vClock, resp.getContentList());

    }

}
