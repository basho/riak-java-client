package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.core.operations.PBFutureOperation;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.PbResultFactory;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakTsPB;
import com.google.protobuf.ByteString;

import java.util.List;

/**
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public class ListKeysOperation extends PBFutureOperation<QueryResult, RiakTsPB.TsListKeysResp, String>
{
    private final Builder builder;
    private String queryInfoMessage;

    private ListKeysOperation(Builder builder)
    {
        super(RiakMessageCodes.MSG_TsListKeysReq,
              RiakMessageCodes.MSG_TsListKeysResp,
              builder.reqBuilder,
              RiakTsPB.TsListKeysResp.PARSER);

        this.builder = builder;
    }

    @Override
    protected QueryResult convert(List<RiakTsPB.TsListKeysResp> rawResponses)
    {
        return PbResultFactory.convertPbListKeysResp(rawResponses);
    }

    @Override
    public String getQueryInfo()
    {
        if (this.queryInfoMessage == null)
        {
            this.queryInfoMessage = createQueryInfoMessage();
        }

        return this.queryInfoMessage;
    }

    private String createQueryInfoMessage()
    {
        return new StringBuilder("SELECT PRIMARY KEY FROM ")
                .append(this.builder.tableName)
                .toString();
    }

    @Override
    protected boolean done(RiakTsPB.TsListKeysResp message)
    {
        return message.getDone();
    }

    public static class Builder
    {
        private final RiakTsPB.TsListKeysReq.Builder reqBuilder =
                RiakTsPB.TsListKeysReq.newBuilder();
        private final String tableName;

        /**
         * Construct a builder for a ListKeysOperation.
         * @param tableName The name of the Time Series table in Riak.
         */
        public Builder(String tableName)
        {
            if (tableName == null)
            {
                throw new IllegalArgumentException("Table Name cannot be null");
            }
            reqBuilder.setTable(ByteString.copyFromUtf8(tableName));
            this.tableName = tableName;
        }

        public Builder withTimeout(int timeout)
        {
            if (timeout <= 0)
            {
                throw new IllegalArgumentException("Timeout can not be zero or less");
            }
            reqBuilder.setTimeout(timeout);
            return this;
        }

        public ListKeysOperation build()
        {
            return new ListKeysOperation(this);
        }
    }
}
