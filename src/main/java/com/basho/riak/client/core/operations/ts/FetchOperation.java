package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.core.query.timeseries.*;
import com.basho.riak.protobuf.RiakTsPB;
import com.basho.riak.protobuf.RiakMessageCodes;

import java.util.List;

/**
 * An operation to fetch a single row in a Riak Time Series table.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public class FetchOperation extends DeferredEncodingOperation<QueryResult, RiakTsPB.TsGetResp, String>
{
    private final Builder builder;
    private String queryInfoMessage;

    private FetchOperation(Builder builder)
    {
        super(RiakMessageCodes.MSG_TsGetReq,
              RiakMessageCodes.MSG_TsGetResp,
//              builder.reqBuilder,
              null,
              RiakTsPB.TsGetResp.PARSER);

            this.builder = builder;
    }

    @Override
    protected RiakMessage createChannelMessage() {
        return new RiakMessage(RiakMessageCodes.MSG_TsGetReq, builder);
    }

    @Override
    protected QueryResult convert(List<RiakTsPB.TsGetResp> responses)
    {
        // This is not a streaming op, there will only be one response
        final RiakTsPB.TsGetResp response = checkAndGetSingleResponse(responses);

        return PbResultFactory.convertPbGetResp(response);
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
        final StringBuilder sb = new StringBuilder();

        for (Cell cell: this.builder.keyValues)
        {
            if (sb.length() > 0)
            {
                sb.append(", ");
            }
            sb.append( cell == null ? "NULL" : cell.toString());
        }

        return String.format("SELECT * FROM %s WHERE PRIMARY KEY = { %s }",
                this.builder.tableName, sb.toString());
    }

    public static class Builder
    {
        private final String tableName;
        private final Iterable<Cell> keyValues;

        //private final RiakTsPB.TsGetReq.Builder reqBuilder = RiakTsPB.TsGetReq.newBuilder();

        public Builder(String tableName, Iterable<Cell> keyValues)
        {
            if (tableName == null || tableName.length() == 0)
            {
                throw new IllegalArgumentException("Table Name cannot be null or an empty string.");
            }

            if (keyValues == null || !keyValues.iterator().hasNext())
            {
                throw new IllegalArgumentException("Key Values cannot be null or an empty.");
            }

//            this.reqBuilder.setTable(ByteString.copyFromUtf8(tableName));
//            this.reqBuilder.addAllKey(ConvertibleIterable.asIterablePbCell(keyValues));

            this.tableName = tableName;
            this.keyValues = keyValues;
        }

        public Builder withTimeout(int timeout)
        {
//            this.reqBuilder.setTimeout(timeout);
            return this;
        }

        public FetchOperation build()
        {
            return new FetchOperation(this);
        }

        public String getTableName() {
            return tableName;
        }

        public Iterable<Cell> getKeyValues() {
            return keyValues;
        }
    }
}
