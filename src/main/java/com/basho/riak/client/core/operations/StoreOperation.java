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
package com.basho.riak.client.core.operations;

import com.basho.riak.client.StoreMeta;
import com.basho.riak.client.cap.ConflictResolver;
import com.basho.riak.client.cap.DefaultResolver;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.convert.Converters;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.core.converters.PutRespConverter;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.query.indexes.RiakIndex;
import com.basho.riak.client.query.links.RiakLink;
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.basho.riak.client.core.operations.Operations.checkMessageType;

/**
 * An operation to store a riak object
 *
 * @param <T> the user type of the operation to store
 */
public class StoreOperation<T> extends FutureOperation<T, RiakKvPB.RpbPutResp>
{

    private final ByteArrayWrapper bucket;
    private final T content;
    private ByteArrayWrapper key;
    private ConflictResolver<T> conflictResolver = new DefaultResolver<T>();
    private Converter<T> domainObjectConverter;
    private StoreMeta storeMeta = StoreMeta.newBuilder().build();

    /**
     * Store an object with the given key to the given bucket.
     *
     * @param bucket  the bucket
     * @param key     the object key
     * @param content the object itself
     */
    public StoreOperation(ByteArrayWrapper bucket, ByteArrayWrapper key, T content)
    {

        if ((null == bucket) || bucket.length() == 0)
        {
            throw new IllegalArgumentException("Bucket can not be null or empty");
        }

        if ((null == key) || key.length() == 0)
        {
            throw new IllegalArgumentException("key can not be null or empty");
        }

        if (null == content)
        {
            throw new IllegalArgumentException("content can not be null");
        }

        this.bucket = bucket;
        this.key = key;
        this.content = content;
    }

    /**
     * Store an object to Riak. Riak will generate an unique key.
     *
     * @param bucket  the bucket to which the object will be stored
     * @param content the data to be stored
     */
    public StoreOperation(ByteArrayWrapper bucket, T content)
    {
        this.bucket = bucket;
        this.content = content;
    }

    /**
     * A {@link Converter} to use to convert the data fetched to some other type
     *
     * @param domainObjectConverter the converter to use.
     * @return this
     */
    public StoreOperation<T> withConverter(Converter<T> domainObjectConverter)
    {
        this.domainObjectConverter = domainObjectConverter;
        return this;
    }

    /**
     * The {@link StoreMeta} to use for this fetch operation
     *
     * @param fetchMeta
     * @return this
     */
    public StoreOperation<T> withStoreMeta(StoreMeta fetchMeta)
    {
        this.storeMeta = fetchMeta;
        return this;
    }

    /**
     * The {@link ConflictResolver} to use resole conflicts on fetch, if they exist
     *
     * @param resolver
     * @return
     */
    public StoreOperation<T> withResolver(ConflictResolver<T> resolver)
    {
        this.conflictResolver = resolver;
        return this;
    }

    @Override
    protected T convert(List<RiakKvPB.RpbPutResp> responses) throws ExecutionException
    {

        if (responses.size() != 1)
        {
            throw new IllegalStateException("RpbPutReq expects one response, " + responses.size() + " were received");
        }

        PutRespConverter responseConverter = new PutRespConverter(bucket, key);
        List<RiakObject> riakObjects = responseConverter.convert(responses.get(0));
        List<T> domainObjects = Converters.convert(domainObjectConverter, riakObjects);
        return conflictResolver.resolve(domainObjects);

    }

    @Override
    protected RiakKvPB.RpbPutResp decode(RiakMessage rawMessage)
    {
        checkMessageType(rawMessage, RiakMessageCodes.MSG_PutResp);
        try
        {
            return RiakKvPB.RpbPutResp.parseFrom(rawMessage.getData());
        }
        catch (InvalidProtocolBufferException e)
        {
            throw new IllegalArgumentException(e);
        }
    }

    private String notNull(String s)
    {
        if (null == s)
        {
            return "";
        }
        return s;
    }

    @Override
    protected RiakMessage createChannelMessage()
    {

        RiakKvPB.RpbPutReq.Builder builder = RiakKvPB.RpbPutReq.newBuilder();
        ByteString pbBucket = ByteString.copyFrom(bucket.unsafeGetValue());
        builder.setBucket(pbBucket);

        if (key != null)
        {
            builder.setKey(ByteString.copyFrom(bucket.unsafeGetValue()));
        }

        RiakObject o = domainObjectConverter.fromDomain(content);
        RiakKvPB.RpbContent.Builder contentBuilder = RiakKvPB.RpbContent.newBuilder();
        contentBuilder.setValue(ByteString.copyFrom(o.getValueAsBytes()));
        contentBuilder.setContentType(ByteString.copyFromUtf8(notNull(o.getContentType())));
        contentBuilder.setCharset(ByteString.copyFromUtf8(notNull(o.getCharset())));
        contentBuilder.setContentEncoding(ByteString.copyFromUtf8(notNull(o.getContentType())));
        contentBuilder.setVtag(ByteString.copyFrom(notNull(o.getVtag()).getBytes()));
        contentBuilder.setLastMod((int) o.getLastModified());
        contentBuilder.setDeleted(o.isDeleted());

        for (RiakLink link : o.getLinks())
        {
            contentBuilder.addLinks(
                RiakKvPB.RpbLink.newBuilder()
                    .setBucket(pbBucket)
                    .setTag(ByteString.copyFrom(link.getTagAsBytes().unsafeGetValue()))
                    .setKey(ByteString.copyFrom(link.getKeyAsBytes().unsafeGetValue())));
        }

        for (RiakIndex<?> index : o.getIndexes())
        {
            for (ByteArrayWrapper value : index.rawValues())
            {
                RiakPB.RpbPair.Builder pair = RiakPB.RpbPair.newBuilder();
                pair.setKey(ByteString.copyFrom(index.getFullname().getBytes()));
                pair.setValue(ByteString.copyFrom(value.unsafeGetValue()));
            }
        }

        builder.setContent(contentBuilder);

        if (storeMeta.hasAsis())
        {
            builder.setAsis(storeMeta.getAsis());
        }

        if (storeMeta.hasDw())
        {
            builder.setDw(storeMeta.getDw().getIntValue());
        }

        if (storeMeta.hasIfNoneMatch())
        {
            builder.setIfNoneMatch(storeMeta.getIfNoneMatch());
        }

        if (storeMeta.hasPw())
        {
            builder.setPw(storeMeta.getPw().getIntValue());
        }

        if (storeMeta.hasIfNotModified())
        {
            builder.setIfNotModified(storeMeta.getIfNotModified());
        }

        if (storeMeta.hasReturnBody())
        {
            builder.setReturnBody(storeMeta.getReturnBody());
        }

        if (storeMeta.hasTimeout())
        {
            builder.setTimeout(storeMeta.getTimeout());
        }

        if (storeMeta.hasReturnHead())
        {
            builder.setReturnHead(storeMeta.getReturnHead());
        }

        if (storeMeta.hasW())
        {
            builder.setW(storeMeta.getW().getIntValue());
        }

        if (storeMeta.hasNval())
        {
            builder.setNVal(storeMeta.getNval());
        }

        if (storeMeta.hasSloppyQuorum())
        {
            builder.setSloppyQuorum(storeMeta.getSloppyQuorum());
        }

        RiakKvPB.RpbPutReq req = builder.build();
        return new RiakMessage(RiakMessageCodes.MSG_PutReq, req.toByteArray());

    }


}
