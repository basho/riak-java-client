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

import com.basho.riak.client.cap.BasicVClock;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.core.converters.RiakObjectConverter;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An operation used to fetch an object from Riak.
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class FetchOperation extends FutureOperation<FetchOperation.Response, RiakKvPB.RpbGetResp>
{
    private final RiakKvPB.RpbGetReq.Builder reqBuilder;
    
    private final Logger logger = LoggerFactory.getLogger(FetchOperation.class);

    private FetchOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
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
            logger.error("Invalid message received; {}", e);
            throw new IllegalArgumentException("Invalid message received", e);
        }
    }

    @Override
    protected FetchOperation.Response convert(List<RiakKvPB.RpbGetResp> responses) throws ExecutionException
    {
        // This is not a streaming op, there will only be one response
        if (responses.size() > 1)
        {
            logger.error("Received {} responses when only one was expected.", responses.size());
        }
        
        RiakKvPB.RpbGetResp response = responses.get(0);
        
        FetchOperation.Response.Builder responseBuilder =
                new FetchOperation.Response.Builder();
        
        // If the response is null ... it means not found. Riak only sends 
        // a message code and zero bytes when that's the case. (See: decode() )
        // Because that makes sense!
        if (null == response)
        {
            responseBuilder.withNotFound(true);
        }
        else
        {
            // To unify the behavior of having just a tombstone vs. siblings
            // that include a tombstone, we create an empty object and mark
            // it deleted
            if (response.getContentCount() == 0)
            {
                responseBuilder.addObject(new RiakObject().setDeleted(true));
            }
            else
            {
                responseBuilder.addObjects(RiakObjectConverter.convert(response.getContentList()));
            }
            
            responseBuilder.withVClock(new BasicVClock(response.getVclock().toByteArray()))
                           .withUnchanged(response.hasUnchanged() ? response.getUnchanged() : false);
            
        }
        
        return responseBuilder.build();
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakKvPB.RpbGetReq req = reqBuilder.build();
        return new RiakMessage(RiakMessageCodes.MSG_GetReq, req.toByteArray());
    }
    
    public static class Builder
    {
        private final RiakKvPB.RpbGetReq.Builder reqBuilder = 
            RiakKvPB.RpbGetReq.newBuilder();
        private final ByteArrayWrapper key;
        private final ByteArrayWrapper bucketName;
        private ByteArrayWrapper bucketType;
        
        /**
         * Constructs a builder for a FetchOperation.
         * @param bucketName The name of the bucket for the operation.
         * @param key The key for the operation.
         */
        public Builder(ByteArrayWrapper bucketName, ByteArrayWrapper key)
        {
            if (null == key || key.length() == 0)
            {
                throw new IllegalArgumentException("Key cannot be null or zero length");
            }
            reqBuilder.setKey(ByteString.copyFrom(key.unsafeGetValue()));
            this.key = key;
            
            if (null == bucketName || bucketName.length() == 0)
            {
                throw new IllegalArgumentException("Bucket name cannot be null or zero length");
            }
            reqBuilder.setBucket(ByteString.copyFrom(bucketName.unsafeGetValue()));
            this.bucketName = bucketName;
        }
        
        /**
         * Set the bucket type for the FetchOperation.
         * If not set, "default" is used.
         * @param bucketType the bucket type
         * @return a reference to this object.
         */
        public Builder withBucketType(ByteArrayWrapper bucketType)
        {
            if (null == bucketType || bucketType.length() == 0)
            {
                throw new IllegalArgumentException("Bucket type can not be null or zero length");
            }
            reqBuilder.setType(ByteString.copyFrom(bucketType.unsafeGetValue()));
            this.bucketType = bucketType;
            return this;
        }
        
        /**
         * Set the R value for this FetchOperation.
         * If not set the bucket default is used.
         * @param r the R value.
         * @return a reference to this object.
         */
        public Builder withR(int r)
		{
			reqBuilder.setR(r);
			return this;
		}

        /**
         * Set the PR value for this query.
         * If not set the bucket default is used.
         * @param pr the PR value.
         * @return 
         */
		public Builder withPr(int pr)
		{
			reqBuilder.setPr(pr);
			return this;
		}

        /**
        * Set the not_found_ok value.
        * <p>
        * If true a vnode returning notfound for a key increments the r tally.
        * False is higher consistency, true is higher availability.
        * </p>
        * <p>
        * If not set the bucket default is used.
        * </p>
        * @param notFoundOk the not_found_ok value.
        * @return a reference to this object.
        */
		public Builder withNotFoundOK(boolean notFoundOK)
		{
			reqBuilder.setNotfoundOk(notFoundOK);
			return this;
		}

        /**
        * Set the basic_quorum value.
        * <p>
        * The parameter controls whether a read request should return early in
        * some fail cases. 
        * E.g. If a quorum of nodes has already
        * returned notfound/error, don't wait around for the rest.
        * </p>
        * @param useBasicQuorum the basic_quorum value.
        * @return a reference to this object.
        */
		public Builder withBasicQuorum(boolean useBasicQuorum)
		{
			reqBuilder.setBasicQuorum(useBasicQuorum);
			return this;
		}

        /**
         * Set whether to return tombstones.
         * @param returnDeletedVClock true to return tombstones, false otherwise.
         * @return a reference to this object.
         */
		public Builder withReturnDeletedVClock(boolean returnDeletedVClock)
		{
			reqBuilder.setDeletedvclock(returnDeletedVClock);
			return this;
		}

        /**
         * Return only the metadata.
         * <p>
         * Causes Riak to only return the metadata for the object. The value
         * will be set to null.
         * @param headOnly true to return only metadata. 
         * @return a reference to this object.
         */
		public Builder withHeadOnly(boolean headOnly)
		{
            reqBuilder.setHead(headOnly);
            return this;
		}

        /**
         * Do not return the object if the supplied vclock matches. 
         * @param vclock the vclock to match on
         * @return a refrence to this object. 
         */
		public Builder withIfNotModified(byte[] vclock)
		{
			reqBuilder.setIfModified(ByteString.copyFrom(vclock));
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
         * Set the n_val for this operation.
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
        
        public FetchOperation build()
        {
            return new FetchOperation(this);
        }
        
        
    }
    
    public static class Response
    {
        private final List<RiakObject> objectList;
        private final VClock vclock;
        private final boolean notFound;
        private final boolean unchanged;
        
        private Response(Builder builder)
        {
            this.objectList = builder.objectList;
            this.vclock = builder.vclock;
            this.notFound = builder.notFound;
            this.unchanged = builder.unchanged;
        }
        
        public List<RiakObject> getObjectList()
        {
            return objectList;
        }
        
        public boolean hasVClock()
        {
            return vclock != null;
        }
        
        public VClock getVClock()
        {
            return vclock;
        }
        
        public boolean isNotFound()
        {
            return notFound;
        }
        
        public boolean isUnchanged()
        {
            return unchanged;
        }
        
        static class Builder
        {
            private final List<RiakObject> objectList =
                new LinkedList<RiakObject>();
            private VClock vclock;
            private boolean notFound;
            private boolean unchanged;
            
            Builder()
            {}
            
            Builder addObject(RiakObject object)
            {
                objectList.add(object);
                return this;
            }
            
            Builder addObjects(List<RiakObject> objects)
            {
                objectList.addAll(objects);
                return this;
            }
            
            
            Builder withVClock(VClock vclock)
            {
                this.vclock = vclock;
                return this;
            }
            
            Builder withNotFound(boolean notFound)
            {
                this.notFound = notFound;
                return this;
            }
            
            Builder withUnchanged(boolean unchanged)
            {
                this.unchanged = unchanged;
                return this;
            }
            
            Response build()
            {
                return new Response(this);
            }
        }
    }
    
}
