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

import com.basho.riak.client.FetchMeta;
import com.basho.riak.client.cap.ConflictResolver;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.core.converters.GetRespConverter;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * An operation used to fetch an object from Riak.
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class FetchOperation<T> extends FutureOperation<T, RiakKvPB.RpbGetResp>
{
    private final ByteArrayWrapper bucket;
    private final ByteArrayWrapper key;
    private ByteArrayWrapper bucketType;
    private ConflictResolver<T> conflictResolver;
    private Converter<T> domainObjectConverter;
    private FetchMeta fetchMeta;

    public FetchOperation(ByteArrayWrapper bucket, ByteArrayWrapper key)
    {
        if ((null == bucket) || bucket.length() == 0)
        {
            throw new IllegalArgumentException("Bucket can not be null or empty");
        }

        if ((null == key) || key.length() == 0)
        {
            throw new IllegalArgumentException("key can not be null or empty");
        }

        this.bucket = bucket;
        this.key = key;
    }

     /**
     * Set the bucket type.
     * If unset "default" is used. 
     * @param bucketType the bucket type to use
     * @return A reference to this object.
     */
    public FetchOperation withBucketType(ByteArrayWrapper bucketType)
    {
        if (null == bucketType || bucketType.length() == 0)
        {
            throw new IllegalArgumentException("Bucket type can not be null or zero length");
        }
        this.bucketType = bucketType;
        return this;
    }
    
    /**
     * Sets the {@link ConflictResolver} to be used if Riak returns siblings
     *
     * @param conflictResolver
     * @return this
     */
    public FetchOperation<T> withResolver(ConflictResolver<T> conflictResolver)
    {
        this.conflictResolver = conflictResolver;
        return this;
    }

    /**
     * A {@link Converter} to use to convert the data fetched to some other type
     *
     * @param domainObjectConverter the converter to use.
     * @return this
     */
    public FetchOperation<T> withConverter(Converter<T> domainObjectConverter)
    {
        this.domainObjectConverter = domainObjectConverter;
        return this;
    }

    /**
     * The {@link FetchMeta} to use for this fetch operation
     *
     * @param fetchMeta
     * @return this
     */
    public FetchOperation<T> withFetchMeta(FetchMeta fetchMeta)
    {
        this.fetchMeta = fetchMeta;
        return this;
    }

    @Override
    protected RiakKvPB.RpbGetResp decode(RiakMessage message)
    {
        Operations.checkMessageType(message, RiakMessageCodes.MSG_GetResp);
        
        try
        {
            byte[] data = message.getData();

            if (data.length == 0) // not found
            {
                return null;
            }

            return RiakKvPB.RpbGetResp.parseFrom(data);
        }
        catch (InvalidProtocolBufferException e)
        {
            throw new IllegalArgumentException("Invalid message received", e);
        }
    }

    @Override
    protected T convert(List<RiakKvPB.RpbGetResp> responses) throws ExecutionException
    {
        GetRespConverter respConverter = new GetRespConverter(bucketType, bucket, key);
        List<RiakObject> riakObjects = new LinkedList<RiakObject>();
        for (RiakKvPB.RpbGetResp response : responses)
        {
            riakObjects.addAll(respConverter.convert(response));
        }

        List<T> convertedObjects = new ArrayList<T>(riakObjects.size());
        for (RiakObject ro : riakObjects)
        {
            convertedObjects.add(domainObjectConverter.toDomain(ro));
        }

        if (convertedObjects.isEmpty())
        {
            return null;
        }
        else if (convertedObjects.size() == 1)
        {
            return convertedObjects.get(0);
        }
        else
        {
            return conflictResolver.resolve(convertedObjects);
        }
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        if (null == fetchMeta)
        {
            fetchMeta = new FetchMeta.Builder().build();
        }

        RiakKvPB.RpbGetReq.Builder builder = RiakKvPB.RpbGetReq.newBuilder();
        builder.setBucket(ByteString.copyFrom(bucket.unsafeGetValue()));
        builder.setKey(ByteString.copyFrom(key.unsafeGetValue()));
        if (bucketType != null)
        {
            builder.setType(ByteString.copyFrom(bucketType.unsafeGetValue()));
        }

        if (fetchMeta.hasHeadOnly())
        {
            builder.setHead(fetchMeta.getHeadOnly());
        }

        if (fetchMeta.hasReturnDeletedVClock())
        {
            builder.setDeletedvclock(fetchMeta.getReturnDeletedVClock());
        }

        if (fetchMeta.hasBasicQuorum())
        {
            builder.setBasicQuorum(fetchMeta.getBasicQuorum());
        }

        if (fetchMeta.hasIfModifiedVClock())
        {
            builder.setIfModified(ByteString.copyFrom(fetchMeta.getIfModifiedVClock().getBytes()));
        }

        if (fetchMeta.hasNotFoundOk())
        {
            builder.setNotfoundOk(fetchMeta.getNotFoundOK());
        }

        if (fetchMeta.hasPr())
        {
            builder.setPr(fetchMeta.getPr().getIntValue());
        }

        if (fetchMeta.hasR())
        {
            builder.setR(fetchMeta.getR().getIntValue());
        }

        RiakKvPB.RpbGetReq req = builder.build();
        return new RiakMessage(RiakMessageCodes.MSG_GetReq, req.toByteArray());

    }
}
