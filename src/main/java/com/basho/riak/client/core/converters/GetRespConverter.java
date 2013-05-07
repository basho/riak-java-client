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
import com.basho.riak.client.query.indexes.RiakIndexes;
import com.basho.riak.client.util.Constants;
import com.basho.riak.client.util.http.DateParseException;
import com.basho.riak.client.util.http.DateUtils;
import com.basho.riak.client.util.http.HttpMultipart;
import com.basho.riak.client.util.http.LinkHeader;
import com.basho.riak.client.util.pb.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakPB.RpbPair;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class GetRespConverter implements RiakResponseConverter<List<RiakObject>>
{

    private final String key;
    private final String bucket;
    private final boolean isHeadRequest;

    public GetRespConverter(String bucket, String key, boolean isHeadRequest)
    {
        this.bucket = bucket;
        this.key = key;
        this.isHeadRequest = isHeadRequest;
    }

    @Override
    public List<RiakObject> convert(HttpResponse response, byte[] content) throws ExecutionException
    {
        int statusCode = response.getStatus().code();
        HttpHeaders headers = response.headers();

        if (statusCode == 300 && !isHeadRequest) // Siblings
        {
            String contentType = headers.get(Constants.HDR_CONTENT_TYPE);

            if (contentType == null || !(contentType.trim().toLowerCase().startsWith(Constants.CTYPE_MULTIPART_MIXED)))
            {
                throw new ExecutionException("multipart/mixed content expected when object has siblings", null);
            }

            return parseMultipart(headers, content);
        }
        else if ( (statusCode >= 200 && statusCode <300) ||
                  (statusCode == 404 && headers.contains(Constants.HDR_VCLOCK)) ||
                  (statusCode == 300 && isHeadRequest ))
        {
            
            // There is a bug in Riak where the x-riak-deleted header is not returned
            // with a tombstone on a 404 (the x-riak-vclock header will exist where it 
            // won't on a "real" 404). The following block and the above conditional 
            // can be changed once that is fixed. 
            if (statusCode == 404) {
                headers.add(Constants.HDR_DELETED, "true");
                content = new byte[0]; // otherwise this will be "not found"
            }
            
            List<RiakLink> links = parseLinkHeader(headers.get(Constants.HDR_LINK));
            RiakIndexes indexes = parseIndexHeaders(new HashSet<Map.Entry<String,String>>(headers.entries()));

            DefaultRiakObject.Builder builder = new DefaultRiakObject.Builder()
                .withBucket(bucket)
                .withKey(key)
                .withValue(content)
                .withVClock(new BasicVClock(headers.get(Constants.HDR_VCLOCK)))
                .withContentType(headers.get(Constants.HDR_CONTENT_TYPE))
                .withLinks(links)
                .withIndexes(indexes)
                .withUsermeta(parseUserMeta(new HashSet<Map.Entry<String,String>>(headers.entries())))
                .withVtag(headers.get(Constants.HDR_ETAG))
                .withDeleted(headers.get(Constants.HDR_DELETED) != null ? true : false);

            if (headers.contains(Constants.HDR_LAST_MODIFIED))
            {
                try
                {
                    Date lastmod = DateUtils.parseDate(headers.get(Constants.HDR_LAST_MODIFIED), 
                                                   DateUtils.DEFAULT_PATTERNS);
                    builder.withLastModified(lastmod.getTime());
                }
                catch (DateParseException e) {}
            }

            ArrayList<RiakObject> objectList = new ArrayList<RiakObject>(1);
            objectList.add(builder.build());
            return objectList;
        }
        else if (statusCode == 304) // conditional 
        {
            RiakObject ro = new DefaultRiakObject.Builder()
                            .withBucket(bucket)
                            .withKey(key)
                            .withVtag(headers.get(Constants.HDR_ETAG))
                            .withModified(false)
                            .build();
            
            ArrayList<RiakObject> objectList = new ArrayList<RiakObject>(1);
            objectList.add(ro);
            return objectList;
            
        } 
        else // statusCode has to be 404 here - see RiakHttpMessageHandler
        {
            RiakObject ro = new DefaultRiakObject.Builder()
                                .withBucket(bucket)
                                .withKey(key)
                                .withNotFound(true)
                                .build();
            
            ArrayList<RiakObject> objectList = new ArrayList<RiakObject>(1);
            objectList.add(ro);
            return objectList;
        }

    }

    @Override
    public List<RiakObject> convert(byte pbMessageCode, byte[] data) throws ExecutionException
    {
        if (RiakMessageCodes.MSG_GetResp != pbMessageCode)
        {
            throw new ExecutionException("Wrong response; expected "
                + RiakMessageCodes.MSG_GetResp
                + " received " + pbMessageCode, null);
        }
        else if (data.length == 0) // not found
        {
            List<RiakObject> objectList = new ArrayList<RiakObject>(1);
            objectList.add(new DefaultRiakObject.Builder()
                                .withKey(key)
                                .withBucket(bucket)
                                .withNotFound(true)
                                .withModified(false)
                                .build()
                            );
            return objectList;
        }
        else // We have a reply and it isn't not-found
        {
            try
            {
                RiakKvPB.RpbGetResp resp = RiakKvPB.RpbGetResp.parseFrom(data);
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
                    DefaultRiakObject.Builder builder =
                        new DefaultRiakObject.Builder()
                        .withKey(key)
                        .withBucket(bucket)
                        .withValue(content.getValue().toByteArray())
                        .withVClock(new BasicVClock(vclock.toByteArray()))
                        .withContentType(nullSafeByteStringToUtf8(content.getContentType()))
                        .withCharset(nullSafeByteStringToUtf8(content.getCharset()))
                        .withVtag(nullSafeByteStringToUtf8(content.getVtag()))
                        .withDeleted(content.getDeleted())
                        .withModified(!resp.getUnchanged());

                    if (content.getLinksCount() > 0)
                    {
                        builder.withLinks(decodeLinks(content));
                    }

                    if (content.hasLastMod())
                    {
                        int lastMod = content.getLastMod();
                        int lastModUsec = content.getLastModUsecs();
                        builder.withLastModified((lastMod * 1000L) + (lastModUsec / 1000L));
                    }

                    if (content.getUsermetaCount() > 0)
                    {
                        Map<String, String> userMeta = new LinkedHashMap<String, String>();
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

                    objectList.add(builder.build());
                }

                return objectList;
            }
            catch (InvalidProtocolBufferException ex)
            {
                throw new ExecutionException(ex);
            }
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

    private List<RiakObject> parseMultipart(HttpHeaders headers, byte[] content)
    {
        String vclock = headers.get(Constants.HDR_VCLOCK);
        List<HttpMultipart.Part> parts = HttpMultipart.parse(headers, content);
        List<RiakObject> objectList = new ArrayList<RiakObject>();
        if (parts != null)
        {
            for (HttpMultipart.Part part : parts)
            {
                Map<String, String> partHeaders = part.getHeaders();
                List<RiakLink> links = parseLinkHeader(partHeaders.get(Constants.HDR_LINK));
                RiakIndexes indexes = parseIndexHeaders(partHeaders.entrySet());

                DefaultRiakObject.Builder builder = new DefaultRiakObject.Builder()
                    .withBucket(bucket)
                    .withKey(key)
                    .withValue(part.getBody())
                    .withVClock(new BasicVClock(vclock))
                    .withContentType(partHeaders.get(Constants.HDR_CONTENT_TYPE))
                    .withLinks(links)
                    .withIndexes(indexes)
                    .withUsermeta(parseUserMeta(partHeaders.entrySet()))
                    .withVtag(partHeaders.get(Constants.HDR_ETAG))
                    .withDeleted(partHeaders.get(Constants.HDR_DELETED) != null ? true : false);
                 
                if (partHeaders.containsKey(Constants.HDR_LAST_MODIFIED))
                {
                    try
                    {
                        Date lastmod = DateUtils.parseDate(partHeaders.get(Constants.HDR_LAST_MODIFIED), 
                                                       DateUtils.DEFAULT_PATTERNS);
                        builder.withLastModified(lastmod.getTime());
                    }
                    catch (DateParseException e) {}
                }
                
                objectList.add(builder.build());
            }
        }

        return objectList;
    }

    private Map<String, String> parseUserMeta(Set<Map.Entry<String,String>> entrySet)
    {
        Map<String, String> usermeta = new HashMap<String, String>();
        
        for (Map.Entry<String, String> e : entrySet)
        {
            String header = e.getKey();
            if (header != null && header.toLowerCase().startsWith(Constants.HDR_USERMETA_PREFIX))
            {
                usermeta.put(header.substring(Constants.HDR_USERMETA_PREFIX.length()), e.getValue());
            }
        }
        
        return usermeta;
    }

    private List<RiakLink> parseLinkHeader(String header)
    {
        List<RiakLink> links = new ArrayList<RiakLink>();
        Map<String, List<Map<String, String>>> parsedLinks = LinkHeader.parse(header);
        for (Map.Entry<String, List<Map<String, String>>> e : parsedLinks.entrySet())
        {
            String url = e.getKey();
            for (Map<String, String> params : e.getValue())
            {
                RiakLink link = null;
                String tag = params.get(Constants.LINK_TAG);
                if (tag != null)
                {
                    String[] parts = url.split("/");
                    if (parts.length >= 2)
                    {
                        link = new RiakLink(parts[parts.length - 2], parts[parts.length - 1], tag);
                    }
                }

                if (link != null)
                {
                    links.add(link);
                }
            }
        }
        return links;
    }

    private RiakIndexes parseIndexHeaders(Set<Map.Entry<String, String>> entryList)
    {
        RiakIndexes indexes = new RiakIndexes();

        for (Map.Entry<String, String> e : entryList)
        {
            String header = e.getKey();
            if (header.toLowerCase().startsWith(Constants.HDR_SEC_INDEX_PREFIX))
            {
                String name = header.substring(Constants.HDR_SEC_INDEX_PREFIX.length());
                String[] values = e.getValue().split(",");

                if (name.endsWith(BinIndex.SUFFIX))
                {
                    indexes.addBinSet(name, new HashSet<String>(Arrays.asList(values)));
                }
                else if (name.endsWith(IntIndex.SUFFIX))
                {
                    Set<Long> set = new HashSet<Long>();
                    for (String s : values)
                    {
                        set.add(Long.parseLong(s));
                    }
                    indexes.addIntSet(name, set);
                }
            }
        }

        return indexes;

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