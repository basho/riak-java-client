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

import com.basho.riak.client.api.cap.VClock;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.ByteString;

import java.util.List;

import static com.basho.riak.client.core.operations.Operations.checkMessageType;
import com.basho.riak.client.core.query.Location;

/**
 * An operation to delete a riak object
 *
 * @author David Rusek <drusek at basho dot com>
 * @since 2.0
 */
public class DeleteOperation extends FutureOperation<Void, Void, Location>
{

    private final RiakKvPB.RpbDelReq.Builder reqBuilder;
    private final Location location;

    private DeleteOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
        this.location = builder.location;
    }

    @Override
    protected Void convert(List<Void> rawResponse) 
    {
        return null;
    }

    @Override
    protected Void decode(RiakMessage rawResponse)
    {
        checkMessageType(rawResponse, RiakMessageCodes.MSG_DelResp);
        return null;
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        return new RiakMessage(RiakMessageCodes.MSG_DelReq, reqBuilder.build().toByteArray());
    }

    @Override
    public Location getQueryInfo()
    {
        return location;
    }

    public static class Builder
    {
        private final Location location;
        private final RiakKvPB.RpbDelReq.Builder reqBuilder = RiakKvPB.RpbDelReq.newBuilder();

        /**
         * Construct a builder for a DeleteOperation.
         * @param location Location of the object in Riak to delete
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
         * Set the W value for this query.
         * If not asSet the bucket default is used.
         *
         * @param w the W value.
         * @return a reference to this object.
         */
        public Builder withW(int w)
        {
            reqBuilder.setW(w);
            return this;
        }

        /**
         * Set the DW value for this query.
         * If not asSet the bucket default is used.
         *
         * @param dw the DW value.
         * @return a reference to this object.
         */
        public Builder withDw(int dw)
        {
            reqBuilder.setDw(dw);
            return this;
        }

        /**
         * Set the PW value for this query.
         * If not asSet the bucket default is used.
         *
         * @param pw the PW value.
         * @return a reference to this object.
         */
        public Builder withPw(int pw)
        {
            reqBuilder.setPw(pw);
            return this;
        }

        /**
         * Set the RW value for this query.
         * If not asSet the bucket default is used.
         *
         * @param rw the RW value.
         * @return a reference to this object.
         */
        public Builder withRw(int rw)
        {
            reqBuilder.setRw(rw);
            return this;
        }

        /**
         * Set the Vclock to be used for this query.
         * If not asSet siblings may be created depending on bucket properties.
         *
         * @param vclock the last fetched vclock.
         * @return a reference to this object.
         */
        public Builder withVclock(VClock vclock)
        {
            reqBuilder.setVclock(ByteString.copyFrom(vclock.getBytes()));
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

        public DeleteOperation build()
        {
            return new DeleteOperation(this);
        }

    }
}
