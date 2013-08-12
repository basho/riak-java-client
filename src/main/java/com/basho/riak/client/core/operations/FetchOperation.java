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
import com.basho.riak.client.core.Protocol;
import com.basho.riak.client.core.RiakPbMessage;
import com.basho.riak.client.core.RiakResponse;
import com.basho.riak.client.core.converters.GetRespConverter;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.pb.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * An operation used to fetch an object from Riak.
 * 
 * @author Brian Roach <roach at basho dot com>
 * @author Russel Brown <russelldb at basho dot com>
 * @since 2.0
 */
public class FetchOperation<T> extends FutureOperation<T>
{
    private final ByteString bucket;
    private final ByteString key;
    private ConflictResolver<T> conflictResolver;
    private Converter<T> domainObjectConverter;
    private FetchMeta fetchMeta;
    
    public FetchOperation(ByteString bucket, ByteString key)
    {
        if ((null == bucket) || bucket.isEmpty())
        {
            throw new IllegalArgumentException("Bucket can not be null or empty");
        }
        
        if ((null == key) || key.isEmpty())
        {
            throw new IllegalArgumentException("key can not be null or empty");
        }

        this.bucket = bucket;
        this.key = key;
    }

    /**
     * Sets the {@link ConflictResolver} to be used if Riak returns siblings
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
     * @param fetchMeta
     * @return this
     */
    public FetchOperation<T> withFetchMeta(FetchMeta fetchMeta)
    {
        this.fetchMeta = fetchMeta;
        return this;
    }
    
    @Override
    protected T convert(RiakResponse rawResponse) throws ExecutionException
    {
        List<RiakObject> riakObjectList = 
            rawResponse.convertResponse(
                new GetRespConverter(bucket, key, fetchMeta.hasHeadOnly() ? fetchMeta.getHeadOnly() : false)
            );
        
        List<T> convertedObjects = new ArrayList<T>(riakObjectList.size());
        
        for (RiakObject ro : riakObjectList)
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
    protected Object createChannelMessage(Protocol p)
    {
        if (null == fetchMeta)
        {
            fetchMeta = new FetchMeta.Builder().build();
        }
        
        // TODO: If we care about multi-ptotocol support, this is kinda ugly
        // Should be tied to the protocol enum or a factory or something
        switch(p)
        {
            case PB:
                return pbChannelMessage(fetchMeta);
            default:
                throw new IllegalArgumentException("Protocol not supported: " + p);
        }
    }
    
    
    
    private Object pbChannelMessage(FetchMeta fetchMeta)
    {
        RiakKvPB.RpbGetReq.Builder builder = RiakKvPB.RpbGetReq.newBuilder();
        builder.setBucket(bucket);
        builder.setKey(key);
        
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
        return new RiakPbMessage(RiakMessageCodes.MSG_GetReq ,req.toByteArray());
        
    }    
}
