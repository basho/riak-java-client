package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.core.codec.InvalidTermToBinaryException;
import com.basho.riak.client.core.codec.TermToBinaryCodec;
import com.basho.riak.client.core.operations.TTBFutureOperation;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.ericsson.otp.erlang.OtpErlangDecodeException;
import com.ericsson.otp.erlang.OtpOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

class TTBConverters
{
    private static Logger logger = LoggerFactory.getLogger(TTBConverters.class);

    private static abstract class BuilderTTBEncoder<T> implements TTBFutureOperation.TTBEncoder
    {
        protected final T builder;

        BuilderTTBEncoder(T builder)
        {
            this.builder = builder;
        }

        abstract OtpOutputStream buildMessage();

        @Override
        public byte[] build()
        {
            return buildMessage().toByteArray();
        }
    }

    static class StoreEncoder extends BuilderTTBEncoder<StoreOperation.Builder>
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

    static class FetchEncoder extends BuilderTTBEncoder<FetchOperation.Builder>
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
            return TermToBinaryCodec.encodeTsGetRequest(builder.getTableName(), list, builder.getTimeout());
        }
    }

    static class QueryEncoder extends BuilderTTBEncoder<QueryOperation.Builder>
    {
        QueryEncoder(QueryOperation.Builder builder)
        {
            super(builder);
        }

        @Override
        OtpOutputStream buildMessage()
        {
            return TermToBinaryCodec.encodeTsQueryRequest(builder.getQueryText(), builder.getCoverageContext());
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

            try
            {
                rv = TermToBinaryCodec.decodeTsResultResponse(data);
            }
            catch (OtpErlangDecodeException | InvalidTermToBinaryException ex)
            {
                final String errorMsg = "Error decoding Riak TTB response";
                logger.error(errorMsg, ex);
                throw new IllegalArgumentException(errorMsg, ex);
            }

            return rv;
        }
    }
}
