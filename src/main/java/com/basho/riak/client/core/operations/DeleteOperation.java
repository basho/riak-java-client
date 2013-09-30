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

import com.basho.riak.client.DeleteMeta;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.ByteString;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.basho.riak.client.core.operations.Operations.checkMessageType;

/**
 * An operation to delete a riak object
 *
 * @author David Rusek <drusek at basho dot com>
 * @since 2.0
 */
public class DeleteOperation extends FutureOperation<Void, Void>
{

	private final ByteArrayWrapper bucket;
	private final ByteArrayWrapper key;
	private DeleteMeta deleteMeta;

    /**
     *
     * @param bucket
     * @param key
     */
	public DeleteOperation(ByteArrayWrapper bucket, ByteArrayWrapper key)
	{

        if ((null == bucket) || bucket.length() == 0)
        {
            throw new IllegalArgumentException("Bucket can not be null or empty");
        }

        if ((null == key) || key.length() == 0)
        {
            throw new IllegalArgumentException("key can not be null or empty");
        }

		this.bucket = bucket;
		this.key = key;
	}

	/**
	 * Sets the {@link DeleteMeta} to be used. If not set, the default options will be used
	 *
	 * @param deleteMeta
	 * @return this
	 */
	public DeleteOperation withDeleteMeta(DeleteMeta deleteMeta)
	{
		this.deleteMeta = deleteMeta;
		return this;
	}

    @Override
    protected Void convert(List<Void> rawResponse) throws ExecutionException
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

		if (null == deleteMeta)
		{
			deleteMeta = new DeleteMeta.Builder().build();
		}

		RiakKvPB.RpbDelReq.Builder builder = RiakKvPB.RpbDelReq.newBuilder();
		builder.setBucket(ByteString.copyFrom(bucket.unsafeGetValue()));
		builder.setKey(ByteString.copyFrom(key.unsafeGetValue()));

		if (deleteMeta.hasTimeout())
		{
			builder.setTimeout(deleteMeta.getTimeout());
		}

		if (deleteMeta.hasPw())
		{
			builder.setPw(deleteMeta.getPw().getIntValue());
		}

		if (deleteMeta.hasDw())
		{
			builder.setDw(deleteMeta.getDw().getIntValue());
		}

		if (deleteMeta.hasPr())
		{
			builder.setPr(deleteMeta.getPr().getIntValue());
		}

		if (deleteMeta.hasPw())
		{
			builder.setPw(deleteMeta.getPw().getIntValue());
		}

		if (deleteMeta.hasR())
		{
			builder.setR(deleteMeta.getR().getIntValue());
		}

		if (deleteMeta.hasRw())
		{
			builder.setRw(deleteMeta.getRw().getIntValue());
		}

		if (deleteMeta.hasW())
		{
			builder.setW(deleteMeta.getW().getIntValue());
		}

		if (deleteMeta.hasVclock())
		{
			builder.setVclock(ByteString.copyFrom(deleteMeta.getVclock().getBytes()));
		}

		RiakKvPB.RpbDelReq req = builder.build();
		return new RiakMessage(RiakMessageCodes.MSG_DelReq, req.toByteArray());

	}
}
