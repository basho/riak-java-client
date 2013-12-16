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
import com.basho.riak.client.query.crdt.types.CrdtElement;
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakDtPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class DtFetchOperation extends FutureOperation<DtFetchOperation.Response, RiakDtPB.DtFetchResp>
{

    private final RiakDtPB.DtFetchReq.Builder reqBuilder;

    private DtFetchOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
    }

    @Override
    protected Response convert(List<RiakDtPB.DtFetchResp> rawResponse) throws ExecutionException
    {
        if (rawResponse.size() != 1)
        {
            throw new IllegalStateException("Expecting exactly one response, instead received " + rawResponse.size());
        }

        RiakDtPB.DtFetchResp response = rawResponse.iterator().next();

        CrdtResponseConverter converter = new CrdtResponseConverter();
        CrdtElement element = converter.convert(response);

        Response.Builder responseBuilder = new Response.Builder()
            .withCrdtElement(element);

        if (response.hasContext())
        {
            ByteArrayWrapper ctxWrapper = ByteArrayWrapper.create(response.getContext().toByteArray());
            responseBuilder.withContext(ctxWrapper);
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
        Operations.checkMessageType(rawMessage, RiakMessageCodes.MSG_DtFetchResp);
        try
        {
            return RiakDtPB.DtFetchResp.parseFrom(rawMessage.getData());
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new IllegalArgumentException("Invalid message received", ex);
        }
    }

    public static class Builder
    {
        private final RiakDtPB.DtFetchReq.Builder reqBuilder = RiakDtPB.DtFetchReq.newBuilder();

        public Builder(ByteArrayWrapper bucket, ByteArrayWrapper key)
        {

            if ((null == bucket) || bucket.length() == 0)
            {
                throw new IllegalArgumentException("Bucket can not be null or empty");
            }

            if ((null == key) || key.length() == 0)
            {
                throw new IllegalArgumentException("key can not be null or empty");
            }

            reqBuilder.setBucket(ByteString.copyFrom(bucket.unsafeGetValue()));
            reqBuilder.setKey(ByteString.copyFrom(key.unsafeGetValue()));
        }

        /**
         * Set the bucket type for the FetchOperation.
         * If not set, "default" is used.
         *
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
            return this;
        }

        /**
         * Set whether a context should be returned
         * Default to false
         *
         * @param context return context
         * @return
         */
        public Builder includeContext(boolean context)
        {
            reqBuilder.setIncludeContext(context);
            return this;
        }

        /**
         * Set the R value for this FetchOperation.
         * If not set the bucket default is used.
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
         * If not set the bucket default is used.
         *
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
        private final ByteArrayWrapper context;
        private final CrdtElement crdtElement;

        protected Response(Init<?> builder)
        {
            this.context = builder.context;
            this.crdtElement = builder.crdtElement;
        }

        public boolean hasContext()
        {
            return context != null;
        }

        public ByteArrayWrapper getContext()
        {
            return context;
        }

        public boolean hasCrdtElement()
        {
            return crdtElement != null;
        }

        public CrdtElement getCrdtElement()
        {
            return crdtElement;
        }

        protected static abstract class Init<T extends Init<T>>
        {
            private ByteArrayWrapper context;
            private CrdtElement crdtElement;
            
            protected abstract T self();
            
            T withContext(ByteArrayWrapper context)
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
            
            T withCrdtElement(CrdtElement crdtElement)
            {
                this.crdtElement = crdtElement;
                return self();
            }
            
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
