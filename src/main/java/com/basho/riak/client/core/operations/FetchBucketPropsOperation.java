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

import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.core.converters.BucketPropertiesConverter;
import com.basho.riak.client.query.BucketProperties;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class FetchBucketPropsOperation extends FutureOperation<FetchBucketPropsOperation.Response, RiakPB.RpbGetBucketResp>
{
    private final RiakPB.RpbGetBucketReq.Builder reqBuilder;
    private final Location location;
    
    public FetchBucketPropsOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
        this.location = builder.location;
    }
    
    @Override
    protected Response convert(List<RiakPB.RpbGetBucketResp> rawResponse) throws ExecutionException
    {
        // This isn't streaming, there will only be one response. 
        RiakPB.RpbBucketProps pbProps = rawResponse.get(0).getProps();
        return new Response.Builder()
                    .withLocation(location)
                    .withBucketProperties(BucketPropertiesConverter.convert(pbProps))
                    .build();
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakPB.RpbGetBucketReq req = reqBuilder.build();
        return new RiakMessage(RiakMessageCodes.MSG_GetBucketReq, req.toByteArray());
    }

    @Override
    protected RiakPB.RpbGetBucketResp decode(RiakMessage rawMessage)
    {
        Operations.checkMessageType(rawMessage, RiakMessageCodes.MSG_GetBucketResp);
        try
        {
            return RiakPB.RpbGetBucketResp.parseFrom(rawMessage.getData());
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new IllegalArgumentException("Invalid message received", ex);
        }
    }
    
    public static class Builder
    {
        private final RiakPB.RpbGetBucketReq.Builder reqBuilder = 
            RiakPB.RpbGetBucketReq.newBuilder();
        private final Location location;
        
        /**
         * Construct a builder for a FetchBucketPropsOperation.
         * @param location The location of the bucket.
         */
        public Builder(Location location)
        {
            if (location == null)
            {
                throw new IllegalArgumentException("Location cannot be null");
            }
            reqBuilder.setBucket(ByteString.copyFrom(location.getBucketName().unsafeGetValue()));
            reqBuilder.setType(ByteString.copyFrom(location.getBucketType().unsafeGetValue()));
            this.location = location;
        }
        
        public FetchBucketPropsOperation build()
        {
            return new FetchBucketPropsOperation(this);
        }
    }
    
    public static class Response extends ResponseWithLocation
    {
        private final BucketProperties props;
        private Response(Init<?> builder)
        {
            super(builder);
            this.props = builder.props;
        }
        
        public BucketProperties getBucketProperties()
        {
            return props;
        }
        
        protected static abstract class Init<T extends Init<T>> extends ResponseWithLocation.Init<T>
        {
            private BucketProperties props;
            
            T withBucketProperties(BucketProperties props)
            {
                this.props = props;
                return self();
            }
        }
        
        static class Builder extends Init<Builder>
        {
            @Override
            public Builder self()
            {
                return this;
            }
            
            @Override
            public Response build()
            {
                return new Response(this);
            }
        }
    }
}
