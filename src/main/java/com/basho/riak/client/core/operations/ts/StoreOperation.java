package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.core.operations.PBFutureOperation;
import com.basho.riak.client.core.query.timeseries.CollectionConverters;
import com.basho.riak.client.core.query.timeseries.ColumnDescription;
import com.basho.riak.client.core.query.timeseries.ConvertibleIterable;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakTsPB;
import com.google.protobuf.ByteString;

import java.util.Collection;
import java.util.List;

/**
 * An operation to store rows to a Riak Time Series table.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public class StoreOperation extends PBFutureOperation<Void, RiakTsPB.TsPutResp, BinaryValue>
{
    private final String tableName;
    private final int rowCount;
    private BinaryValue queryInfoMessage;


    private StoreOperation(Builder builder)
    {
        super(RiakMessageCodes.MSG_TsPutReq,
              RiakMessageCodes.MSG_TsPutResp,
              builder.reqBuilder,
              RiakTsPB.TsPutResp.PARSER);

        this.tableName = builder.reqBuilder.getTable().toStringUtf8();
        this.rowCount = builder.reqBuilder.getRowsCount();
    }

    @Override
    protected Void convert(List<RiakTsPB.TsPutResp> responses)
    {
        // This is not a streaming op, there will only be one response
        checkAndGetSingleResponse(responses);

        return null;
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
        return BinaryValue.create("INSERT <" + this.rowCount + " rows> into " + this.tableName);
    }

    public static class Builder
    {
        private final RiakTsPB.TsPutReq.Builder reqBuilder;

        public Builder(BinaryValue tableName)
        {
            if (tableName == null || tableName.length() == 0)
            {
                throw new IllegalArgumentException("TableName can not be null or empty");
            }

            this.reqBuilder = RiakTsPB.TsPutReq.newBuilder();
            this.reqBuilder.setTable(ByteString.copyFrom(tableName.unsafeGetValue()));
        }

        public Builder withColumns(Collection<ColumnDescription> columns)
        {
            this.reqBuilder.addAllColumns(CollectionConverters.convertColumnDescriptionsToPb(columns));
            return this;
        }

        public Builder withRows(Collection<Row> rows)
        {
            this.reqBuilder.addAllRows(ConvertibleIterable.asIterablePbRow(rows));
            return this;
        }

        public StoreOperation build()
        {
            return new StoreOperation(this);
        }
    }
}


