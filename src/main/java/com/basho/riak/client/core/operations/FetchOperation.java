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

import com.basho.riak.client.api.cap.BasicVClock;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.core.converters.RiakObjectConverter;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An operation used to fetch an object from Riak.
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class FetchOperation extends FutureOperation<FetchOperation.Response, RiakKvPB.RpbGetResp, Location>
{
    private final RiakKvPB.RpbGetReq.Builder reqBuilder;
    Location location;

    private final Logger logger = LoggerFactory.getLogger(FetchOperation.class);

    private FetchOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
        this.location = builder.location;
    }

    @Override
    protected RiakKvPB.RpbGetResp decode(RiakMessage message)
    {
        Operations.checkPBMessageType(message, RiakMessageCodes.MSG_GetResp);

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
    protected FetchOperation.Response convert(List<RiakKvPB.RpbGetResp> responses) {
        // This is not a streaming op, there will only be one response
        if (responses.size() > 1)
        {
            logger.error("Received {} responses when only one was expected.", responses.size());
        }

        final RiakKvPB.RpbGetResp response = responses.get(0);
        return convert(response);
    }

    static FetchOperation.Response convert(RiakKvPB.RpbGetResp response)
    {
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
                RiakObject ro = new RiakObject()
                                    .setDeleted(true)
                                    .setVClock(new BasicVClock(response.getVclock().toByteArray()));

                responseBuilder.addObject(ro);
            }
            else
            {
                responseBuilder.addObjects(RiakObjectConverter.convert(response.getContentList(), response.getVclock()));
            }

            responseBuilder.withUnchanged(response.hasUnchanged() ? response.getUnchanged() : false);

        }

        return responseBuilder.build();
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        RiakKvPB.RpbGetReq req = reqBuilder.build();
        return new RiakMessage(RiakMessageCodes.MSG_GetReq, req.toByteArray());
    }

    @Override
    public Location getQueryInfo()
    {
        return location;
    }

    public static class Builder
    {
        private final RiakKvPB.RpbGetReq.Builder reqBuilder =
            RiakKvPB.RpbGetReq.newBuilder();
        private final Location location;

        /**
         * Construct a FetchOperation that will retrieve an object from Riak stored
         * at the provided Location.
         * @param location the location of the object in Riak to fetch.
         */
        public Builder(Location location)
        {
            if (location == null)
            {
                throw new IllegalArgumentException("Location can not be null.");
            }

            reqBuilder.setKey(ByteString.copyFrom(location.getKey().unsafeGetValue()));
            reqBuilder.setBucket(ByteString.copyFrom(location.getNamespace().getBucketName().unsafeGetValue()));
            reqBuilder.setType(ByteString.copyFrom(location.getNamespace().getBucketType().unsafeGetValue()));
            this.location = location;

        }

        /**
         * Set the R value for this FetchOperation.
         * If not asSet the bucket default is used.
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
         * If not asSet the bucket default is used.
         * @param pr the PR value.
         * @return a reference to this object.
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
        * If not asSet the bucket default is used.
        * </p>
        * @param notFoundOk the not_found_ok value.
        * @return a reference to this object.
        */
		public Builder withNotFoundOK(boolean notFoundOk)
		{
			reqBuilder.setNotfoundOk(notFoundOk);
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
         * will be asSet to null.
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

    protected static abstract class KvResponseBase
    {
        private final List<RiakObject> objectList;

        protected KvResponseBase(Init<?> builder)
        {
            this.objectList = builder.objectList;
        }

        public List<RiakObject> getObjectList()
        {
            return objectList;
        }

        protected static abstract class Init<T extends Init<T>>
        {
            private final List<RiakObject> objectList =
                new LinkedList<RiakObject>();
            protected abstract T self();
            protected abstract KvResponseBase build();

            T addObject(RiakObject object)
            {
                objectList.add(object);
                return self();
            }

            T addObjects(List<RiakObject> objects)
            {
                objectList.addAll(objects);
                return self();
            }
        }
    }


    public static class Response extends KvResponseBase
    {
        private final boolean notFound;
        private final boolean unchanged;

        private Response(Init<?> builder)
        {
            super(builder);
            this.notFound = builder.notFound;
            this.unchanged = builder.unchanged;
        }

        public boolean isNotFound()
        {
            return notFound;
        }

        public boolean isUnchanged()
        {
            return unchanged;
        }

        protected static abstract class Init<T extends Init<T>> extends KvResponseBase.Init<T>
        {
            private boolean notFound;
            private boolean unchanged;

            T withNotFound(boolean notFound)
            {
                this.notFound = notFound;
                return self();
            }

            T withUnchanged(boolean unchanged)
            {
                this.unchanged = unchanged;
                return self();
            }
        }

        static class Builder extends Init<Builder>
        {
            @Override
            protected Builder self()
            {
                return this;
            }

            @Override
            protected Response build()
            {
                return new Response(this);
            }
        }
    }
}
