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

import com.basho.riak.client.core.query.crdt.ops.*;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.core.converters.CrdtResponseConverter;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.crdt.types.RiakDatatype;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakDtPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.List;

public class DtUpdateOperation extends FutureOperation<DtUpdateOperation.Response, RiakDtPB.DtUpdateResp, Location>
{
    private final Location location;
    private final RiakDtPB.DtUpdateReq.Builder reqBuilder;

    private DtUpdateOperation(Builder builder)
    {
        this.reqBuilder = builder.reqBuilder;
        this.location = builder.location;
    }

    @Override
    protected Response convert(List<RiakDtPB.DtUpdateResp> rawResponse)
    {
        if (rawResponse.size() != 1)
        {
            throw new IllegalStateException("Expecting exactly one response, instead received " + rawResponse.size());
        }

        RiakDtPB.DtUpdateResp response = rawResponse.iterator().next();
        CrdtResponseConverter converter = new CrdtResponseConverter();
        RiakDatatype element = converter.convert(response);

        Response.Builder responseBuilder =
            new Response.Builder().withCrdtElement(element);

        if (response.hasKey())
        {
            BinaryValue key = BinaryValue.unsafeCreate(response.getKey().toByteArray());
            responseBuilder.withGeneratedKey(key);
        }

        if (response.hasContext())
        {
            BinaryValue context = BinaryValue.unsafeCreate(response.getContext().toByteArray());
            responseBuilder.withContext(context);
        }

        return responseBuilder.build();
    }

    @Override
    protected RiakMessage createChannelMessage()
    {
        return new RiakMessage(RiakMessageCodes.MSG_DtUpdateReq, reqBuilder.build().toByteArray());
    }

    @Override
    protected RiakDtPB.DtUpdateResp decode(RiakMessage rawMessage)
    {
        Operations.checkPBMessageType(rawMessage, RiakMessageCodes.MSG_DtUpdateResp);
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

    @Override
    public Location getQueryInfo()
    {
        return location;
    }

    public static class Builder
    {
        private final RiakDtPB.DtUpdateReq.Builder reqBuilder = RiakDtPB.DtUpdateReq.newBuilder();
        private final Location location;
        private boolean removeOpPresent = false;

        /**
         * Construct a builder for a DtUpdateOperation.
         * @param location The location of the object in Riak.
         */
        public Builder(Location location)
        {
            if (location == null)
            {
                throw new IllegalArgumentException("Location cannot be null");
            }
            else if (location.getNamespace().getBucketTypeAsString().equals(Namespace.DEFAULT_BUCKET_TYPE))
            {
                throw new IllegalArgumentException("Default bucket type does not accept CRDTs");
            }

            reqBuilder.setBucket(ByteString.copyFrom(location.getNamespace().getBucketName().unsafeGetValue()));
            reqBuilder.setType(ByteString.copyFrom(location.getNamespace().getBucketType().unsafeGetValue()));
            reqBuilder.setKey(ByteString.copyFrom(location.getKey().unsafeGetValue()));

            this.location = location;
        }

        public Builder(Namespace namespace)
        {
            if (namespace == null)
            {
                throw new IllegalArgumentException("Namespace cannot be null");
            }
            else if (namespace.getBucketTypeAsString().equals(Namespace.DEFAULT_BUCKET_TYPE))
            {
                throw new IllegalArgumentException("Default bucket type does not accept CRDTs");
            }

            // This is simply for the returned query info
            Location loc = new Location(namespace, "RIAK_GENERATED");

            reqBuilder.setBucket(ByteString.copyFrom(loc.getNamespace().getBucketName().unsafeGetValue()));
            reqBuilder.setType(ByteString.copyFrom(loc.getNamespace().getBucketType().unsafeGetValue()));

            this.location = loc;
        }

        /**
         * Set the context for this operation.
         *
         * @param ctx a context from a previous fetch operation.
         * @return a reference to this object.
         */
        public Builder withContext(BinaryValue ctx)
        {
            if (null == ctx)
            {
                throw new IllegalArgumentException("Context cannot be null.");
            }
            reqBuilder.setContext(ByteString.copyFrom(ctx.unsafeGetValue()));
            return this;
        }

        /**
         * Set the W value for this DtUpdateOperation.
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
         * Set the DW value for this DtUpdateOperation.
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
         * Set the PW value for this DtUpdateOperation.
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
         * Return the object after storing (including any siblings).
         *
         * @param returnBody true to return the object.
         * @return a reference to this object.
         */
        public Builder withReturnBody(boolean returnBody)
        {
            reqBuilder.setReturnBody(returnBody);
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
         * Set the n_val value for this operation.
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

        public DtUpdateOperation build()
        {
            if (removeOpPresent && !reqBuilder.hasContext())
            {
                throw new IllegalStateException("Remove operations cannot be performed without a context.");
            }

            return new DtUpdateOperation(this);
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

            for (BinaryValue element : op.getAdds())
            {
                setOpBuilder.addAdds(ByteString.copyFrom(element.unsafeGetValue()));
            }

            for (BinaryValue element : op.getRemoves())
            {
                setOpBuilder.addRemoves(ByteString.copyFrom(element.unsafeGetValue()));
            }

            if (setOpBuilder.getRemovesCount() > 0)
            {
                removeOpPresent = true;
            }
            return setOpBuilder.build();
        }

        RiakDtPB.HllOp getHllOp(HllOp op)
        {
            RiakDtPB.HllOp.Builder hllOpBuilder = RiakDtPB.HllOp.newBuilder();

            for (BinaryValue element : op.getElements())
            {
                hllOpBuilder.addAdds(ByteString.copyFrom(element.unsafeGetValue()));
            }

            return hllOpBuilder.build();
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

            for (MapOp.MapField field : op.getRemoves())
            {
                mapOpBuilder.addRemoves(getMapField(field));
            }

            if (mapOpBuilder.getRemovesCount() > 0)
            {
                removeOpPresent = true;
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

        /**
         * Add an update op to this operation
         *
         * @param op the update
         * @return this
         */
        public Builder withOp(CrdtOp op)
        {
            if (op instanceof CounterOp)
            {
                withOp((CounterOp) op);
            }
            else if (op instanceof MapOp)
            {
                withOp((MapOp) op);
            }
            else if (op instanceof SetOp)
            {
                withOp((SetOp) op);
            }
            else if (op instanceof HllOp)
            {
                withOp((HllOp) op);
            }

            return this;
        }

        private Builder withOp(CounterOp op)
        {
            reqBuilder.setOp(RiakDtPB.DtOp.newBuilder()
                .setCounterOp(getCounterOp(op)));

            return this;
        }

        private Builder withOp(MapOp op)
        {
            reqBuilder.setOp(RiakDtPB.DtOp.newBuilder()
                .setMapOp(getMapOp(op)));
            return this;
        }

        private Builder withOp(SetOp op)
        {
            reqBuilder.setOp(RiakDtPB.DtOp.newBuilder()
                .setSetOp(getSetOp(op)));

            return this;
        }

        private Builder withOp(HllOp op)
        {
            reqBuilder.setOp(RiakDtPB.DtOp.newBuilder()
                .setHllOp(getHllOp(op)));
            return this;
        }
    }

    public static class Response extends DtFetchOperation.Response
    {
        private final BinaryValue generatedKey;

        private Response(Init<?> builder)
        {
            super(builder);
            this.generatedKey = builder.generatedKey;
        }

        public boolean hasGeneratedKey()
        {
            return generatedKey != null;
        }

        public BinaryValue getGeneratedKey()
        {
            return generatedKey;
        }

        protected static abstract class Init<T extends Init<T>> extends DtFetchOperation.Response.Init<T>
        {
            private BinaryValue generatedKey;

            T withGeneratedKey(BinaryValue generatedKey)
            {
                this.generatedKey = generatedKey;
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
