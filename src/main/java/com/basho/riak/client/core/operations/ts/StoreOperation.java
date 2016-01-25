package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.core.query.timeseries.ColumnDescription;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakTsPB;

import java.util.Collection;
import java.util.List;

/**
 * An operation to store rows to a Riak Time Series table.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public class StoreOperation extends DeferredEncodingOperation<Void, RiakTsPB.TsPutResp, String>
{
    private final Builder builder;
    private String queryInfoMessage;


    private StoreOperation(Builder builder)
    {
        super(RiakMessageCodes.MSG_TsPutReq,
              RiakMessageCodes.MSG_TsPutResp,
//              builder.reqBuilder,
              null,
              RiakTsPB.TsPutResp.PARSER);

        this.builder = builder;

//        this.tableName = builder.reqBuilder.getTable().toStringUtf8();
//        this.rowCount = builder.reqBuilder.getRowsCount();
    }

    @Override
    protected RiakMessage createChannelMessage() {
        return new RiakMessage(RiakMessageCodes.MSG_TsPutReq, builder);
    }

    @Override
    protected Void convert(List<RiakTsPB.TsPutResp> responses)
    {
        // This is not a streaming op, there will only be one response
        checkAndGetSingleResponse(responses);

        return null;
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
        return "INSERT into " + builder.tableName;
    }

    public static class Builder
    {
        private final String tableName;
        private Iterable<Row> rows;

//        private final RiakTsPB.TsPutReq.Builder reqBuilder;

        public Builder(String tableName)
        {
            if (tableName == null || tableName.length() == 0)
            {
                throw new IllegalArgumentException("TableName can not be null or empty");
            }

            this.tableName = tableName;
//            this.reqBuilder = RiakTsPB.TsPutReq.newBuilder();
//            this.reqBuilder.setTable(ByteString.copyFromUtf8(tableName));
        }

        public Builder withColumns(Collection<ColumnDescription> columns)
        {
//            this.reqBuilder.addAllColumns(CollectionConverters.convertColumnDescriptionsToPb(columns));
//            return this;
            throw new UnsupportedOperationException();
        }

        public Builder withRows(Iterable<Row> rows)
        {
//            this.reqBuilder.addAllRows(ConvertibleIterable.asIterablePbRow(rows));
            this.rows = rows;
            return this;
        }

        public String getTableName() {
            return tableName;
        }

        public Iterable<Row> getRows() {
            return rows;
        }

        public StoreOperation build()
        {
            return new StoreOperation(this);
        }
    }
}


