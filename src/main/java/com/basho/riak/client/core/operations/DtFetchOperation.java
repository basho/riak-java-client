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

import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.core.converters.CrdtResponseConverter;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.crdt.types.RiakDatatype;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakDtPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.List;

public class DtFetchOperation extends FutureOperation<DtFetchOperation.Response, RiakDtPB.DtFetchResp, Location>
{
    private final Location location;
    private final RiakDtPB.DtFetchReq.Builder reqBuilder;

    private DtFetchOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
        this.location = builder.location;
    }

    @Override
    protected Response convert(List<RiakDtPB.DtFetchResp> rawResponse)
    {
        if (rawResponse.size() != 1)
        {
            throw new IllegalStateException("Expecting exactly one response, instead received " + rawResponse.size());
        }

        RiakDtPB.DtFetchResp response = rawResponse.iterator().next();

        CrdtResponseConverter converter = new CrdtResponseConverter();
        RiakDatatype element = converter.convert(response);

        Response.Builder responseBuilder = new Response.Builder()
            .withCrdtElement(element);

        if (response.hasContext())
        {
            BinaryValue ctxWrapper = BinaryValue.create(response.getContext().toByteArray());
            responseBuilder.withContext(ctxWrapper);
        }

        // If we don't have a value set, then the Data Type is considered "New" or "Not Found"
        // A null context could be misleading, since the request might specify to not return a context.
        // See riak_dt.proto#DtFetchResp L113-132
        if(!response.hasValue())
        {
            responseBuilder.withNotFound(true);
        }

        return responseBuilder.build();
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        return new RiakMessage(RiakMessageCodes.MSG_DtFetchReq, reqBuilder.build().toByteArray());
    }

    @Override
    protected RiakDtPB.DtFetchResp decode(RiakMessage rawMessage)
    {
        Operations.checkPBMessageType(rawMessage, RiakMessageCodes.MSG_DtFetchResp);
        try
        {
            return RiakDtPB.DtFetchResp.parseFrom(rawMessage.getData());
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new IllegalArgumentException("Invalid message received", ex);
        }
    }

    @Override
    public Location getQueryInfo()
    {
        return location;
    }

    public static class Builder
    {
        private final RiakDtPB.DtFetchReq.Builder reqBuilder = RiakDtPB.DtFetchReq.newBuilder();
        private final Location location;

        /**
         * Construct a Builder for a DtFetchOperaiton
         * @param location the location of the object in Riak.
         */
        public Builder(Location location)
        {
            if (location == null)
            {
                throw new IllegalArgumentException("Location can not be null");
            }

            reqBuilder.setBucket(ByteString.copyFrom(location.getNamespace().getBucketName().unsafeGetValue()));
            reqBuilder.setKey(ByteString.copyFrom(location.getKey().unsafeGetValue()));
            reqBuilder.setType(ByteString.copyFrom(location.getNamespace().getBucketType().unsafeGetValue()));
            this.location = location;
        }

        /**
         * Set whether a context should be returned.
         * Defaults to true.
         *
         * @param context return context
         * @return a reference to this object.
         */
        public Builder includeContext(boolean context)
        {
            reqBuilder.setIncludeContext(context);
            return this;
        }

        /**
         * Set the R value for this FetchOperation.
         * If not asSet the bucket default is used.
         *
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
         *
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
         *
         * @param notFoundOK the not_found_ok value.
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
         *
         * @param useBasicQuorum the basic_quorum value.
         * @return a reference to this object.
         */
        public Builder withBasicQuorum(boolean useBasicQuorum)
        {
            reqBuilder.setBasicQuorum(useBasicQuorum);
            return this;
        }

        /**
         * Set a timeout for this operation.
         *
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
         *
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
         *
         * @param sloppyQuorum true to use sloppy_quorum
         * @return a reference to this object.
         */
        public Builder withSloppyQuorum(boolean sloppyQuorum)
        {
            reqBuilder.setSloppyQuorum(sloppyQuorum);
            return this;
        }

        public DtFetchOperation build()
        {
            return new DtFetchOperation(this);
        }

    }

    public static class Response
    {
        private final BinaryValue context;
        private final RiakDatatype crdtElement;
        private final boolean isNotFound;

        protected Response(Init<?> builder)
        {
            this.context = builder.context;
            this.crdtElement = builder.crdtElement;
            this.isNotFound = builder.isNotFound;
        }

        public boolean hasContext()
        {
            return context != null;
        }

        public BinaryValue getContext()
        {
            return context;
        }

        public boolean hasCrdtElement()
        {
            return crdtElement != null;
        }

        public RiakDatatype getCrdtElement()
        {
            return crdtElement;
        }

        public boolean isNotFound()
        {
            return this.isNotFound;
        }

        protected static abstract class Init<T extends Init<T>>
        {
            private BinaryValue context;
            private RiakDatatype crdtElement;
            private boolean isNotFound;

            protected abstract T self();
            protected abstract Response build();

            T withContext(BinaryValue context)
            {
                if (context != null)
                {
                    if (context.length() == 0)
                    {
                        throw new IllegalArgumentException("Context cannot be null or zero length");
                    }
                    else
                    {
                        this.context = context;
                    }
                }
                return self();
            }

            T withCrdtElement(RiakDatatype crdtElement)
            {
                this.crdtElement = crdtElement;
                return self();
            }

            T withNotFound(boolean notFound)
            {
                this.isNotFound = notFound;
                return self();
            }
        }


        private static class Builder extends Init<Builder>
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
