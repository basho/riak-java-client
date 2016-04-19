package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.core.codec.TermToBinaryCodec;
import com.basho.riak.client.core.operations.TTBFutureOperation;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.ericsson.otp.erlang.OtpOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import java.util.LinkedList;

class TTBConverters
{
    private static abstract class MemoizingEncoder<T> implements TTBFutureOperation.TTBEncoder
    {
        // private final byte reqMessageCode = RiakMessageCodes.MSG_TsTtbMsg;
        protected byte[] message = null;
        protected final T builder;

        MemoizingEncoder(T builder)
        {
            this.builder = builder;
        }

        abstract OtpOutputStream buildMessage();

        @Override
        public byte[] build()
        {
            if (message == null)
            {
                OtpOutputStream os = buildMessage();
                os.flush();
                // TODO GH-611 should the output stream or base type be returned?
                // TODO GH-611 we still need to add msgCode 141 and message length
                final int msgLen = os.length() + 5; // Add 4-byte msg length and 1-byte msgCode
                ByteArrayOutputStream bs = new ByteArrayOutputStream(msgLen);
                DataOutputStream ds = new DataOutputStream(bs);
                ds.writeInt(msgLen);
                ds.writeByte(RiakMessageCodes.MSG_TsTtbMsg);
                ds.flush();
                os.writeTo(bs);
                bs.flush();
                message = bs.toByteArray();
            }
            return message;
        }
    }

    static class StoreEncoder extends MemoizingEncoder<StoreOperation.Builder>
    {
        StoreEncoder(StoreOperation.Builder builder)
        {
            super(builder);
        }

        @Override
        OtpOutputStream buildMessage()
        {
            return TermToBinaryCodec.encodeTsPutRequest(builder.getTableName(), builder.getRows());
        }
    }

    static class FetchEncoder extends MemoizingEncoder<FetchOperation.Builder>
    {
        FetchEncoder(FetchOperation.Builder builder)
        {
            super(builder);
        }

        @Override
        OtpOutputStream buildMessage()
        {
            // TODO: Remove this later
            LinkedList<Cell> list = new LinkedList<>();
            for (Cell c : builder.getKeyValues())
            {
                list.add(c);
            }
            return TermToBinaryCodec.encodeTsGetRequest(builder.getTableName(),
                                                        list,
                                                        builder.getTimeout());
        }
    }

    static class QueryEncoder extends MemoizingEncoder<QueryOperation.Builder>
    {
        QueryEncoder(QueryOperation.Builder builder)
        {
            super(builder);
        }

        @Override
        OtpOutputStream buildMessage()
        {
            return TermToBinaryCodec.encodeTsQueryRequest(builder.getQueryText());
        }
    }

    static class VoidDecoder implements TTBFutureOperation.TTBParser<Void>
    {
        @Override
        public Void parseFrom(byte[] data)
        {
            return null;
        }
    }

    static class QueryResultDecoder implements TTBFutureOperation.TTBParser<QueryResult>
    {
        @Override
        public QueryResult parseFrom(byte[] data)
        {
            return TermToBinaryCodec.decodeTsGetResponse(data);
        }
    }
}
