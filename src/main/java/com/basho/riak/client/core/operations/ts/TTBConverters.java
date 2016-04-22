package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.api.RiakException;
import com.basho.riak.client.core.codec.InvalidTermToBinaryException;
import com.basho.riak.client.core.codec.TermToBinaryCodec;
import com.basho.riak.client.core.operations.TTBFutureOperation;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.ericsson.otp.erlang.OtpErlangDecodeException;
import com.ericsson.otp.erlang.OtpOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

class TTBConverters
{
    private static abstract class MemoizingEncoder<T> implements TTBFutureOperation.TTBEncoder
    {
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
                try {
                    OtpOutputStream os = buildMessage();
                    os.flush();
                    message = os.toByteArray();
                } catch (IOException ex) {
                    // TODO GH-611
                    Logger.getLogger(TTBConverters.class.getName()).log(Level.SEVERE, null, ex);
                }
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
            QueryResult rv;

            try {
                rv = TermToBinaryCodec.decodeTsGetResponse(data);
            } catch (OtpErlangDecodeException | InvalidTermToBinaryException ex)
            {
                Logger.getLogger(TTBConverters.class.getName()).log(Level.SEVERE, null, ex);
                throw new IllegalArgumentException("Error decoding Riak TTB Response", ex);
            }

            return rv;
        }
    }
}
