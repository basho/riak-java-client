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
import com.basho.riak.client.query.crdt.ops.*;
import com.basho.riak.client.query.crdt.types.CrdtElement;
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakDtPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class DtUpdateOperation extends FutureOperation<CrdtElement, RiakDtPB.DtUpdateResp>
{

    private final RiakDtPB.DtUpdateReq.Builder builder =
        RiakDtPB.DtUpdateReq.newBuilder();

    /**
     * Store an object to the given bucket.
     *
     * @param bucket the bucket
     */
    public DtUpdateOperation(ByteArrayWrapper bucket)
    {

        if ((null == bucket) || bucket.length() == 0)
        {
            throw new IllegalArgumentException("Bucket can not be null or empty");
        }

        builder.setBucket(ByteString.copyFrom(bucket.unsafeGetValue()));
    }

    /**
     * Set the bucket type.
     * If unset "default" is used.
     *
     * @param bucketType the bucket type to use
     * @return A reference to this object.
     */
    public DtUpdateOperation withBucketType(ByteArrayWrapper bucketType)
    {
        if (null == bucketType || bucketType.length() == 0)
        {
            throw new IllegalArgumentException("Bucket type can not be null or zero length");
        }
        builder.setType(ByteString.copyFrom(bucketType.unsafeGetValue()));
        return this;
    }

    /**
     * (optional) key under which to store the content, if no key is given one will
     * be chosen by Riak and returned
     *
     * @param key
     * @return
     */
    public DtUpdateOperation withKey(ByteArrayWrapper key)
    {
        if ((null == key) || key.length() == 0)
        {
            throw new IllegalArgumentException("key can not be null or empty");
        }

        builder.setKey(ByteString.copyFrom(key.unsafeGetValue()));

        return this;
    }

    /**
     * (optional for some operations) context for CRDT operations
     *
     * @param context
     * @return
     */
    public DtUpdateOperation withContext(ByteArrayWrapper context)
    {
        if ((null == context) || context.length() == 0)
        {
            throw new IllegalArgumentException("key can not be null or empty");
        }

        builder.setContext(ByteString.copyFrom(context.unsafeGetValue()));

        return this;
    }

    public DtUpdateOperation w(Quorum w)
    {
        builder.setW(w.getIntValue());
        return this;
    }

    public DtUpdateOperation dw(Quorum dw)
    {
        builder.setDw(dw.getIntValue());
        return this;
    }

    public DtUpdateOperation pw(Quorum pw)
    {
        builder.setPw(pw.getIntValue());
        return this;
    }

    public DtUpdateOperation timeout(int timeout)
    {
        builder.setTimeout(timeout);
        return this;
    }

    public DtUpdateOperation sloppyQuorum(boolean sloppyQuorum)
    {
        builder.setSloppyQuorum(sloppyQuorum);
        return this;
    }

    public DtUpdateOperation nval(int nval)
    {
        builder.setNVal(nval);
        return this;
    }

    private RiakDtPB.CounterOp getCounterOp(CounterOp op)
    {
        return RiakDtPB.CounterOp.newBuilder()
            .setIncrement(op.getIncrement())
            .build();
    }

    private RiakDtPB.SetOp getSetOp(SetOp op)
    {
        RiakDtPB.SetOp.Builder setOpBuilder = RiakDtPB.SetOp.newBuilder();

        for (ByteArrayWrapper element : op.getAdds())
        {
            setOpBuilder.addAdds(ByteString.copyFrom(element.unsafeGetValue()));
        }

        for (ByteArrayWrapper element : op.getRemoves())
        {
            setOpBuilder.addRemoves(ByteString.copyFrom(element.unsafeGetValue()));
        }

        return setOpBuilder.build();
    }

    private RiakDtPB.MapUpdate.FlagOp getFlagOp(FlagOp op)
    {
        return op.getEnabled()
            ? RiakDtPB.MapUpdate.FlagOp.ENABLE
            : RiakDtPB.MapUpdate.FlagOp.DISABLE;
    }

    private ByteString getRegisterOp(RegisterOp op)
    {
        return ByteString.copyFrom(op.getValue().unsafeGetValue());
    }

    private RiakDtPB.MapField getMapField(MapOp.MapField field)
    {
        return null;
    }

    private RiakDtPB.MapOp getMapOp(MapOp op)
    {
        RiakDtPB.MapOp.Builder mapOpBuilder = RiakDtPB.MapOp.newBuilder();

        for (MapOp.MapField field : op.getAdds())
        {
            mapOpBuilder.addAdds(getMapField(field));
        }

        for (MapOp.MapField field : op.getRemoves())
        {
            mapOpBuilder.addRemoves(getMapField(field));
        }

        for (MapOp.MapUpdate update : op.getUpdates())
        {
            RiakDtPB.MapUpdate.Builder mapUpdateBuilder =
                RiakDtPB.MapUpdate.newBuilder();

            switch (update.field.type)
            {
                case COUNTER:
                    mapUpdateBuilder.setCounterOp(getCounterOp((CounterOp) update.op));
                    break;
                case FLAG:
                    mapUpdateBuilder.setFlagOp(getFlagOp((FlagOp) update.op));
                    break;
                case MAP:
                    mapUpdateBuilder.setMapOp(getMapOp((MapOp) update.op));
                    break;
                case REGISTER:
                    mapUpdateBuilder.setRegisterOp(getRegisterOp((RegisterOp) update.op));
                    break;
                case SET:
                    mapUpdateBuilder.setSetOp(getSetOp((SetOp) update.op));
                    break;
                default:
                    throw new IllegalStateException("Unknow datatype encountered");
            }

            mapOpBuilder.addUpdates(mapUpdateBuilder);
        }

        return mapOpBuilder.build();

    }

    public DtUpdateOperation withOp(CounterOp op)
    {
        builder.setOp(RiakDtPB.DtOp.newBuilder()
            .setCounterOp(getCounterOp(op)));

        return this;
    }

    public DtUpdateOperation withOp(MapOp op)
    {
        builder.setOp(RiakDtPB.DtOp.newBuilder()
            .setMapOp(getMapOp(op)));
        return this;
    }

    public DtUpdateOperation withOp(SetOp op)
    {

        builder.setOp(RiakDtPB.DtOp.newBuilder()
            .setSetOp(getSetOp(op)));

        return this;
    }

    @Override
    protected CrdtElement convert(List<RiakDtPB.DtUpdateResp> rawResponse) throws ExecutionException
    {
        if (rawResponse.size() != 1)
        {
            throw new IllegalStateException("Expecting exactly one response, instead received " + rawResponse.size());
        }

        RiakDtPB.DtUpdateResp response = rawResponse.iterator().next();

        CrdtResponseConverter converter = new CrdtResponseConverter();
        return converter.convert(response);

    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        return new RiakMessage(RiakMessageCodes.MSG_DtUpdateReq, builder.build().toByteArray());
    }

    @Override
    protected RiakDtPB.DtUpdateResp decode(RiakMessage rawMessage)
    {
        Operations.checkMessageType(rawMessage, RiakMessageCodes.MSG_DtUpdateResp);
        try
        {
            return RiakDtPB.DtUpdateResp.parseFrom(rawMessage.getData());
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new IllegalArgumentException("Invalid message received", ex);
        }
    }
}
