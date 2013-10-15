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
import com.basho.riak.client.query.RiakDatatype;
import com.basho.riak.client.query.crdt.ops.*;
import com.basho.riak.client.query.crdt.types.CrdtElement;
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakDtPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class DtUpdateOperation extends FutureOperation<RiakDatatype, RiakDtPB.DtUpdateResp>
{

    private final RiakDtPB.DtUpdateReq.Builder builder =
        RiakDtPB.DtUpdateReq.newBuilder();

    /**
     * Store an object to the given bucket.
     *
     * @param bucket     the bucket
     * @param bucketType the bucket type
     */
    public DtUpdateOperation(ByteArrayWrapper bucket, ByteArrayWrapper bucketType)
    {

        if ((null == bucket) || bucket.length() == 0)
        {
            throw new IllegalArgumentException("Bucket can not be null or empty");
        }

        if (null == bucketType || bucketType.length() == 0)
        {
            throw new IllegalArgumentException("Bucket type can not be null or zero length");
        }

        builder.setBucket(ByteString.copyFrom(bucket.unsafeGetValue()));

        builder.setType(ByteString.copyFrom(bucketType.unsafeGetValue()));
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

    public DtUpdateOperation returnBody(boolean returnBody)
    {
        builder.setReturnBody(returnBody);
        return this;
    }

    RiakDtPB.CounterOp getCounterOp(CounterOp op)
    {
        return RiakDtPB.CounterOp.newBuilder()
            .setIncrement(op.getIncrement())
            .build();
    }

    RiakDtPB.SetOp getSetOp(SetOp op)
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

    RiakDtPB.MapUpdate.FlagOp getFlagOp(FlagOp op)
    {
        return op.getEnabled()
            ? RiakDtPB.MapUpdate.FlagOp.ENABLE
            : RiakDtPB.MapUpdate.FlagOp.DISABLE;
    }

    ByteString getRegisterOp(RegisterOp op)
    {
        return ByteString.copyFrom(op.getValue().unsafeGetValue());
    }

    RiakDtPB.MapField getMapField(MapOp.MapField field)
    {
        RiakDtPB.MapField.Builder mapFieldBuilder = RiakDtPB.MapField.newBuilder();

        switch (field.type)
        {
            case SET:
                mapFieldBuilder.setType(RiakDtPB.MapField.MapFieldType.SET);
                break;
            case REGISTER:
                mapFieldBuilder.setType(RiakDtPB.MapField.MapFieldType.REGISTER);
                break;
            case MAP:
                mapFieldBuilder.setType(RiakDtPB.MapField.MapFieldType.MAP);
                break;
            case FLAG:
                mapFieldBuilder.setType(RiakDtPB.MapField.MapFieldType.FLAG);
                break;
            case COUNTER:
                mapFieldBuilder.setType(RiakDtPB.MapField.MapFieldType.COUNTER);
                break;
            default:
        }
        mapFieldBuilder.setName(ByteString.copyFrom(field.key.unsafeGetValue()));
        return mapFieldBuilder.build();
    }

    RiakDtPB.MapOp getMapOp(MapOp op)
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

            mapUpdateBuilder.setField(getMapField(update.field));
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
    protected RiakDatatype convert(List<RiakDtPB.DtUpdateResp> rawResponse) throws ExecutionException
    {
        if (rawResponse.size() != 1)
        {
            throw new IllegalStateException("Expecting exactly one response, instead received " + rawResponse.size());
        }

        RiakDtPB.DtUpdateResp response = rawResponse.iterator().next();

        CrdtResponseConverter converter = new CrdtResponseConverter();
        CrdtElement element = converter.convert(response);
        ByteArrayWrapper bucket = ByteArrayWrapper.unsafeCreate(builder.getBucket().toByteArray());

        ByteArrayWrapper bucketType = null;
        if (builder.hasType())
        {
            bucketType = ByteArrayWrapper.unsafeCreate(builder.getType().toByteArray());
        }

        ByteArrayWrapper key = null;
        if (response.hasKey())
        {
            key = ByteArrayWrapper.unsafeCreate(response.getKey().toByteArray());
        }

        return new RiakDatatype(bucketType, bucket, key, element);

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
            RiakDtPB.DtUpdateResp resp = RiakDtPB.DtUpdateResp.parseFrom(rawMessage.getData());
            return resp;
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new IllegalArgumentException("Invalid message received", ex);
        }
    }
}
