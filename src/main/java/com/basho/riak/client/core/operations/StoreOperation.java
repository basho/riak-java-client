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

import com.basho.riak.client.cap.BasicVClock;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.core.converters.RiakObjectConverter;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.BinaryValue;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.basho.riak.client.core.operations.Operations.checkMessageType;
import com.basho.riak.client.query.Location;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An operation to store a riak object
 */
public class StoreOperation extends FutureOperation<StoreOperation.Response, RiakKvPB.RpbPutResp>
{
    private final Logger logger = LoggerFactory.getLogger(StoreOperation.class);
    private final RiakKvPB.RpbPutReq.Builder reqBuilder;
    private final Location location;
    
    private StoreOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
        this.location = builder.location;
    }

    @Override
    protected Response convert(List<RiakKvPB.RpbPutResp> responses) throws ExecutionException
    {
        // There should only be one response message from Riak.
        if (responses.size() != 1)
        {
            throw new IllegalStateException("RpbPutReq expects one response, " + responses.size() + " were received");
        }

        RiakKvPB.RpbPutResp response = responses.get(0);
        
        // This only exists if no key was specified in the put request
        if (response.hasKey())
        {
            location.setKey(BinaryValue.unsafeCreate(response.getKey().toByteArray()));
        }
        
        StoreOperation.Response.Builder responseBuilder = 
            new StoreOperation.Response.Builder().withLocation(location);
        
        if (response.hasVclock())
        {
            responseBuilder.withVClock(new BasicVClock(response.getVclock().toByteArray()));
        }
        
        
        if (response.getContentCount() > 0)
        {
            responseBuilder.addObjects(RiakObjectConverter.convert(response.getContentList()));
        }
        
        return responseBuilder.build();
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
            logger.error("Invalid message received; {}", e);
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakKvPB.RpbPutReq req = reqBuilder.build();
        return new RiakMessage(RiakMessageCodes.MSG_PutReq, req.toByteArray());
    }

    public static class Builder
    {
        private final RiakKvPB.RpbPutReq.Builder reqBuilder = RiakKvPB.RpbPutReq.newBuilder();
        private final Location location;
        
        /**
         * Constructs a builder for a StoreOperation
         * @param location The location in Riak at which to store.
         */
        public Builder(Location location)
        {
            if (location == null)
            {
                throw new IllegalArgumentException("Location cannot be null");
            }
            
            reqBuilder.setType(ByteString.copyFrom(location.getBucketType().unsafeGetValue()));
            reqBuilder.setBucket(ByteString.copyFrom(location.getBucketName().unsafeGetValue()));
            
            if (location.hasKey())
            {
                reqBuilder.setKey(ByteString.copyFrom(location.getKey().unsafeGetValue()));
            }
            this.location = location;
        }
        
        public Builder withContent(RiakObject content)
        {
            if (null == content)
            {
                throw new IllegalArgumentException("Object cannot be null.");
            }
            
            reqBuilder.setContent(RiakObjectConverter.convert(content));
            return this;
        }
        
        /**
         * Set the vclock for this operation.
         * @param vclock a vclock from a previous fetch operation.
         * @return a reference to this object.
         */
        public Builder withVClock(VClock vclock)
        {
            if (null == vclock)
            {
                throw new IllegalArgumentException("VClock cannot be null.");
            }
            reqBuilder.setVclock(ByteString.copyFrom(vclock.getBytes()));
            return this;
        }
        
        /**
         * Set the W value for this StoreOperation.
         * If not asSet the bucket default is used.
         * @param w the W value.
         * @return a reference to this object.
         */
        public Builder withW(int w)
		{
			reqBuilder.setW(w);
			return this;
		}

        /**
         * Set the DW value for this StoreOperation.
         * If not asSet the bucket default is used.
         * @param dw the DW value.
         * @return a reference to this object.
         */
		public Builder withDw(int dw)
		{
			reqBuilder.setDw(dw);
			return this;
		}

        /**
         * Set the PW value for this StoreOperation.
         * If not asSet the bucket default is used.
         * @param pw the PW value.
         * @return a reference to this object.
         */
		public Builder withPw(int pw)
		{
			reqBuilder.setPw(pw);
			return this;
		}

        /**
         * Return the object after storing (including any siblings).
         * @param returnBody true to return the object. 
         * @return a reference to this object.
         */
		public Builder withReturnBody(boolean returnBody)
		{
			reqBuilder.setReturnBody(returnBody);
			return this;
		}

        /**
         * Return the metadata after storing the value.
         * <p>
         * Causes Riak to only return the metadata for the object. The value
         * will be asSet to null.
         * @param returnHead true to return only metadata. 
         * @return a reference to this object.
         */
		public Builder withReturnHead(boolean returnHead)
		{
			reqBuilder.setReturnHead(returnHead);
			return this;
		}

        /**
         * Set the if_not_modified flag for this StoreOperation.
         * <p>
         * Setting this to true means to update the value only if the 
         * vclock in the supplied object matches the one in the database.
         * </p>
         * <p>
         * Be aware there are several cases where this may not actually happen.
         * Use of this feature is discouraged.
         * </p>
         * @param ifNotModified
         * @return a reference to this object.
         */
		public Builder withIfNotModified(boolean ifNotModified)
		{
			reqBuilder.setIfNotModified(ifNotModified);
			return this;
		}

        /**
         * Set the if_none_match flag value for this StoreOperation.
         * <p>
         * Setting this to true means store the value only if this 
         * bucket/key combination are not already defined. 
         * </p>
         * Be aware that there are several cases where 
         * this may not actually happen. Use of this option is discouraged.
         * </p>
         * 
         * @param ifNoneMatch the if_non-match value.
         * @return a reference to this object.
         */
		public Builder withIfNoneMatch(boolean ifNoneMatch)
		{
			reqBuilder.setIfNoneMatch(ifNoneMatch);
			return this;
		}

        /**
         * Set the asis value for this operation.
         * <p>
         * <b>Do not use this unless you understand the ramifications</b>
         * </p>
         * @param asis the asis value
         * @return a reference to this object.
         */
		public Builder withAsis(boolean asis)
		{
			reqBuilder.setAsis(asis);
			return this;
		}

        /**
         * Set a timeout for this operation.
         * @param timeout a timeout in milliseconds.
         * @return a reference to this object.
         */
		public Builder withTimeout(int timeout)
		{
			reqBuilder.setTimeout(timeout);
			return this;
		}

        /**
         * Set the n_val value for this operation.
         * <p>
         * <b>Do not use this unless you understand the ramifications</b>
         * </p>
         * @param nval the n_val value
         * @return a reference to this object.
         */
		public Builder withNVal(int nval)
		{
            reqBuilder.setNVal(nval);
            return this;
		}

        /**
         * Set whether to use sloppy_quorum.
         * <p>
         * <b>Do not use this unless you understand the ramifications</b>
         * </p>
         * @param sloppyQuorum true to use sloppy_quorum
         * @return a reference to this object.
         */
		public Builder withSloppyQuorum(boolean sloppyQuorum)
		{
			reqBuilder.setSloppyQuorum(sloppyQuorum);
			return this;
		}
        
        public StoreOperation build()
        {
            return new StoreOperation(this);
        }
        
    }
    
    public static class Response extends FetchOperation.ResponseBase
    {
        private final BinaryValue generatedKey;
        
        private Response(Init<?> builder)
        {
            super(builder);
            this.generatedKey = builder.generatedKey;
}
        
        public boolean hasGeneratedKey()
        {
            return generatedKey != null;
        }
        
        public BinaryValue getGeneratedKey()
        {
            return generatedKey;
        }
        
        protected static abstract class Init<T extends Init<T>> extends FetchOperation.ResponseBase.Init<T>
        {
            private BinaryValue generatedKey;
            
            T withGeneratedKey(BinaryValue generatedKey)
            {
                this.generatedKey = generatedKey;
                return self();
            }
            
            @Override
            Response build()
            {
                return new Response(this);
            }
        }
        
        static class Builder extends Init<Builder>
        {
            @Override
            protected Builder self()
            {
                return this;
            }
        }
    }
}
