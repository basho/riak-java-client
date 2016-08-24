package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.core.operations.TTBFutureOperation;
import com.basho.riak.client.core.query.timeseries.CollectionConverters;
import com.basho.riak.client.core.query.timeseries.ColumnDescription;
import com.basho.riak.client.core.query.timeseries.ConvertibleIterable;
import com.basho.riak.client.core.query.timeseries.Row;
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
public class StoreOperation extends TTBFutureOperation<Void, String>
{
    private final Builder builder;
    private String queryInfoMessage;

    private StoreOperation(final Builder builder)
    {
        super(new TTBConverters.StoreEncoder(builder), new TTBConverters.VoidDecoder());
        this.builder = builder;
    }

    @Override
    protected Void convert(List<byte[]> responses)
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
        private final RiakTsPB.TsPutReq.Builder reqBuilder;
        private final String tableName;
        private Collection<Row> rows;

        public Builder(String tableName)
        {
            if (tableName == null || tableName.length() == 0)
            {
                throw new IllegalArgumentException("TableName can not be null or empty");
            }

            this.tableName = tableName;
            this.reqBuilder = RiakTsPB.TsPutReq.newBuilder();
            this.reqBuilder.setTable(ByteString.copyFromUtf8(tableName));
        }

        public Builder withColumns(Collection<ColumnDescription> columns)
        {
            this.reqBuilder.addAllColumns(CollectionConverters.convertColumnDescriptionsToPb(columns));
            return this;
        }

        public Builder withRows(Collection<Row> rows)
        {
            this.rows = rows;
            this.reqBuilder.addAllRows(ConvertibleIterable.asIterablePbRow(rows));
            return this;
        }

        public String getTableName()
        {
            return tableName;
        }

        public Collection<Row> getRows()
        {
            return rows;
        }

        public StoreOperation build()
        {
            return new StoreOperation(this);
        }
    }
}
