package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.core.operations.PBFutureOperation;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakTsPB;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by alex on 12/3/15.
 */
public class ListKeysOperation extends PBFutureOperation<ListKeysOperation.Response, RiakTsPB.TsListKeysResp, BinaryValue>
{
    private final Builder builder;
    private BinaryValue queryInfoMessage;

    ListKeysOperation(Builder builder)
    {
        super(RiakMessageCodes.MSG_TsListKeysReq,
              RiakMessageCodes.MSG_TsListKeysResp,
              builder.reqBuilder,
              RiakTsPB.TsListKeysResp.PARSER);

        this.builder = builder;
    }

    @Override
    protected Response convert(List<RiakTsPB.TsListKeysResp> rawResponse)
    {
        Response.Builder builder = new Response.Builder();
        for (RiakTsPB.TsListKeysResp resp : rawResponse)
        {
            for (RiakTsPB.TsRow tsRow : resp.getKeysList())
            {
                // TODO: Convert row
                //builder.addRow();
            }
        }
        return builder.build();
    }

    @Override
    public BinaryValue getQueryInfo()
    {
        if (this.queryInfoMessage == null)
        {
            this.queryInfoMessage = createQueryInfoMessage();
        }

        return this.queryInfoMessage;
    }

    private BinaryValue createQueryInfoMessage()
    {
        final StringBuilder sb = new StringBuilder("SELECT PRIMARY KEY FROM ");
        sb.append(this.builder.tableName);

        return BinaryValue.create(sb.toString());
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
        private final BinaryValue tableName;

        /**
         * Construct a builder for a ListKeysOperaiton.
         * @param tableName The name of the Time Series table in Riak.
         */
        public Builder(BinaryValue tableName)
        {
            if (tableName == null)
            {
                throw new IllegalArgumentException("Table Name cannot be null");
            }
            reqBuilder.setTable(ByteString.copyFrom(tableName.getValue()));
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

    public static class Response
    {
        private final List<Row> rows;
        private Response(Builder builder)
        {
            this.rows = builder.rows;
        }

        public List<Row> getRows()
        {
            return rows;
        }

        static class Builder
        {
            private List<Row> rows = new ArrayList<Row>();

            Builder addRows(List<Row> rows)
            {
                this.rows.addAll(rows);
                return this;
            }

            Builder addRow(Row row)
            {
                this.rows.add(row);
                return this;
            }

            Response build()
            {
                return new Response(this);
            }
        }
    }
}
