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

import com.basho.riak.client.FetchMeta;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.basho.riak.client.core.operations.Operations.checkMessageType;

/**
 * An operation to fetch a Riak counter
 *
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public class FetchCounterOperation extends FutureOperation<Long, RiakKvPB.RpbCounterGetResp>
{

	private final ByteArrayWrapper bucket;
	private final ByteArrayWrapper key;
	private FetchMeta fetchMeta = new FetchMeta.Builder().build();

	public FetchCounterOperation(ByteArrayWrapper bucket, ByteArrayWrapper key)
	{
		this.bucket = bucket;
		this.key = key;
	}

	@Override
	protected Long convert(List<RiakKvPB.RpbCounterGetResp> responses) throws ExecutionException
	{
        if (responses.size() != 1)
        {
            throw new IllegalArgumentException("Expecting one and only one response to RpcCounterGetReq");
        }
        RiakKvPB.RpbCounterGetResp response = responses.get(0);
		return response.getValue();

	}

    @Override
    protected RiakKvPB.RpbCounterGetResp decode(RiakMessage rawResponse)
    {
        checkMessageType(rawResponse, RiakMessageCodes.MSG_CounterGetResp);

        try
        {
            return RiakKvPB.RpbCounterGetResp.parseFrom(rawResponse.getData());
        }
        catch (InvalidProtocolBufferException e)
        {
            throw new IllegalArgumentException(e);
        }
    }

    public FetchCounterOperation withFetchMeta(FetchMeta fetchMeta)
	{
		this.fetchMeta = fetchMeta;
		return this;
	}

	@Override
	protected RiakMessage createChannelMessage()
	{

		RiakKvPB.RpbCounterGetReq.Builder builder = RiakKvPB.RpbCounterGetReq.newBuilder();
		builder.setBucket(ByteString.copyFrom(bucket.unsafeGetValue()));
		builder.setKey(ByteString.copyFrom(key.unsafeGetValue()));

		if (fetchMeta.hasR())
		{
			builder.setR(fetchMeta.getR().getIntValue());
		}

		if (fetchMeta.hasPr())
		{
			builder.setPr(fetchMeta.getPr().getIntValue());
		}

		if (fetchMeta.hasBasicQuorum())
		{
			builder.setBasicQuorum(fetchMeta.getBasicQuorum());
		}

		if (fetchMeta.hasNotFoundOk())
		{
			builder.setNotfoundOk(fetchMeta.getNotFoundOK());
		}

		RiakKvPB.RpbCounterGetReq req = builder.build();
		return new RiakMessage(RiakMessageCodes.MSG_CounterGetReq, req.toByteArray());

	}

}
