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

import com.basho.riak.client.cap.Quorum;
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

public class DtFetchOperation extends FutureOperation<CrdtElement, RiakDtPB.DtFetchResp>
{

    private final RiakDtPB.DtFetchReq.Builder builder =
        RiakDtPB.DtFetchReq.newBuilder();

    public DtFetchOperation(ByteArrayWrapper bucket, ByteArrayWrapper key)
    {

        if ((null == bucket) || bucket.length() == 0)
        {
            throw new IllegalArgumentException("Bucket can not be null or empty");
        }

        if ((null == key) || key.length() == 0)
        {
            throw new IllegalArgumentException("key can not be null or empty");
        }

        builder.setBucket(ByteString.copyFrom(bucket.unsafeGetValue()));
        builder.setKey(ByteString.copyFrom(key.unsafeGetValue()));
    }

    /**
     * Set the bucket type.
     * If unset "default" is used.
     *
     * @param bucketType the bucket type to use
     * @return A reference to this object.
     */
    public DtFetchOperation withBucketType(ByteArrayWrapper bucketType)
    {
        if (null == bucketType || bucketType.length() == 0)
        {
            throw new IllegalArgumentException("Bucket type can not be null or zero length");
        }

        builder.setType(ByteString.copyFrom(bucketType.unsafeGetValue()));

        return this;
    }

    public DtFetchOperation includeContext(boolean includeContext)
    {
        builder.setIncludeContext(includeContext);
        return this;
    }

    public DtFetchOperation r(Quorum r)
    {
        builder.setR(r.getIntValue());
        return this;
    }

    public DtFetchOperation pr(Quorum pr)
    {
        builder.setPr(pr.getIntValue());
        return this;
    }

    public DtFetchOperation basicQuorum(boolean basicQuorum)
    {
        builder.setBasicQuorum(basicQuorum);
        return this;
    }

    public DtFetchOperation notFoundOK(boolean notFoundOK)
    {
        builder.setNotfoundOk(notFoundOK);
        return this;
    }

    public DtFetchOperation timeout(int timeout)
    {
        builder.setTimeout(timeout);
        return this;
    }

    public DtFetchOperation sloppyQuorum(boolean sloppyQuorum)
    {
        builder.setSloppyQuorum(sloppyQuorum);
        return this;
    }

    public DtFetchOperation nval(int nval)
    {
        builder.setNVal(nval);
        return this;
    }

    @Override
    protected CrdtElement convert(List<RiakDtPB.DtFetchResp> rawResponse) throws ExecutionException
    {
        if (rawResponse.size() != 1)
        {
            throw new IllegalStateException("Expecting exactly one response, instead received " + rawResponse.size());
        }

        RiakDtPB.DtFetchResp response = rawResponse.iterator().next();

        CrdtResponseConverter converter = new CrdtResponseConverter();
        CrdtElement element = converter.convert(response);
        if (response.hasContext())
        {
            ByteArrayWrapper ctxWrapper = ByteArrayWrapper.create(response.getContext().toByteArray());
            element.setContext(ctxWrapper);
        }

        return element;
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        if (!builder.hasType())
        {
            builder.setType(ByteString.copyFromUtf8("default"));
        }

        return new RiakMessage(RiakMessageCodes.MSG_DtFetchReq, builder.build().toByteArray());
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
}
