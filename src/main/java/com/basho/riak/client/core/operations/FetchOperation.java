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
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.cap.ConflictResolver;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.Protocol;
import com.basho.riak.client.core.RiakPbMessage;
import com.basho.riak.client.core.RiakResponse;
import com.basho.riak.client.core.converters.GetRespConverter;
import com.basho.riak.client.util.Constants;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.client.util.UrlEncoder;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.ByteString;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * An operation used to fetch an object from Riak.
 * 
 * @author Brian Roach <roach at basho dot com>
 * @author Russel Brown <russelldb at basho dot com>
 * @since 2.0
 */
// TODO: Head only request
public class FetchOperation<T> extends FutureOperation<T>
{
    private final byte[] bucket;
    private final byte[] key;
    private ConflictResolver<T> conflictResolver;
    private Converter<T> domainObjectConverter;
    private FetchMeta fetchMeta;
    
    public FetchOperation(byte[] bucket, byte[] key)
    {
        if (null == bucket)
        {
            throw new IllegalArgumentException("Bucket can not be null");
        }
        
        if (null == key)
        {
            throw new IllegalArgumentException("key can not be null");
        }

        
        this.bucket = Arrays.copyOf(bucket, bucket.length);
        this.key = Arrays.copyOf(key, key.length);
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
     * @param converter
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
        if (fetchMeta.hasIfModifiedSince())
        {
            if (fetchMeta.getIfModifiedSince() != null)
            {
                supportedProtocols(Protocol.HTTP);
            }
            else
            {
                supportedProtocols(Protocol.PB);
            }
        }
        return this;
    }
    
    @Override
    protected T convert(RiakResponse rawResponse) throws UnresolvedConflictException
    {
        List<RiakObject> riakObjectList = 
            rawResponse.convertResponse(new GetRespConverter(bucket, key));
        
        List<T> convertedObjects = new ArrayList<T>(riakObjectList.size());
        
        for (RiakObject ro : riakObjectList)
        {
            convertedObjects.add(domainObjectConverter.toDomain(ro));
        }
        
        return conflictResolver.resolve(convertedObjects);
    }

    @Override
    protected Object createChannelMessage(Protocol p)
    {
        if (null == fetchMeta)
        {
            fetchMeta = new FetchMeta.Builder().build();
        }
        
        switch(p)
        {
            case HTTP:
                return httpChannelMessage(fetchMeta);
            case PB:
                return pbChannelMessage(fetchMeta);
            default:
                throw new IllegalArgumentException("Protocol not supported: " + p);
        }
    }
    
    private Object httpChannelMessage(FetchMeta fetchMeta)
    {
        
        StringBuilder uriBuilder = new StringBuilder("/buckets/");
        
        uriBuilder.append(UrlEncoder.urlEncode(bucket));
        uriBuilder.append("/keys/");
        uriBuilder.append(UrlEncoder.urlEncode(key));
        uriBuilder.append(buildParamString(fetchMeta));
        
        HttpRequest message = 
            new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uriBuilder.toString());
        
        String hostname = "localhost";
        try
        {
            hostname = InetAddress.getLocalHost().getHostName();
        }
        catch (UnknownHostException e)
        {}
        
        message.headers().set(HttpHeaders.Names.HOST, hostname);
        message.headers().set(HttpHeaders.Names.ACCEPT, Constants.CTYPE_ANY + ", " + Constants.CTYPE_MULTIPART_MIXED);
        
        Date modifiedSince = fetchMeta.getIfModifiedSince();
        if (modifiedSince != null)
        {
            SimpleDateFormat sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");
            message.headers().set(HttpHeaders.Names.IF_MODIFIED_SINCE, sdf.format(modifiedSince));
        }
        
        return message;
    }
    
    private Object pbChannelMessage(FetchMeta fetchMeta)
    {
        RiakKvPB.RpbGetReq.Builder builder = RiakKvPB.RpbGetReq.newBuilder();
        builder.setBucket(ByteString.copyFrom(bucket));
        builder.setKey(ByteString.copyFrom(key));
        
        if (fetchMeta.hasReturnDeletedVClock())
        {
            builder.setDeletedvclock(fetchMeta.getReturnDeletedVClock());
        }
        
        if (fetchMeta.hasBasicQuorum())
        {
            builder.setBasicQuorum(fetchMeta.getBasicQuorum());
        }
        
        if (fetchMeta.hasIfModifiedSince())
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
    
    private String buildParamString(FetchMeta fetchMeta)
    {
        StringBuilder paramBuilder = new StringBuilder("?");
        
        if (fetchMeta.hasR())
        {
            Quorum quorum = fetchMeta.getR();
            paramBuilder.append(Constants.QP_R);
            paramBuilder.append("=");
            if (quorum.isSymbolic())
            {
                paramBuilder.append(quorum.getName());
            }
            else
            {
                paramBuilder.append(quorum.getIntValue());
            }
            paramBuilder.append("&");
        }
        
        if (fetchMeta.hasPr())
        {
            Quorum quorum = fetchMeta.getPr();
            paramBuilder.append(Constants.QP_PR);
            paramBuilder.append("=");
            if (quorum.isSymbolic())
            {
                paramBuilder.append(quorum.getName());
            }
            else
            {
                paramBuilder.append(quorum.getIntValue());
            }
            paramBuilder.append("&");
        }
        
        if (fetchMeta.hasNotFoundOk())
        {
            paramBuilder.append(Constants.QP_NOT_FOUND_OK);
            paramBuilder.append("=");
            paramBuilder.append(fetchMeta.getNotFoundOK().toString());
            paramBuilder.append("&");
        }
        
        if (fetchMeta.hasBasicQuorum())
        {
            paramBuilder.append(Constants.QP_BASIC_QUORUM);
            paramBuilder.append("=");
            paramBuilder.append(fetchMeta.getBasicQuorum().toString());
            paramBuilder.append("&");
        }
        
        if (paramBuilder.length() == 1)
        {
            // only has '?'
            return "";
        }
        else
        {
            // Discard trailing '&'
            return paramBuilder.substring(0, paramBuilder.length() - 1);
        }
    }
}
