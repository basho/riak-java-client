package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.core.operations.TTBFutureOperation;
import com.basho.riak.client.core.query.timeseries.CollectionConverters;
import com.basho.riak.client.core.query.timeseries.ColumnDescription;
import com.basho.riak.client.core.query.timeseries.ConvertibleIterable;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.protobuf.RiakTsPB;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
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
        private final String tableName;
        private Collection<Row> rows;
        private Collection<String> columns;

        public Builder(String tableName)
        {
            if (tableName == null || tableName.length() == 0)
            {
                throw new IllegalArgumentException("TableName can not be null or empty");
            }

            this.tableName = tableName;
        }

        /**
         * Add the names & order of the columns to be inserted.
         * Order is implied by the order of the names in the Collection.
         * <b>NOTE:</b>: As of Riak TS 1.4, this functionality is not implemented server-side,
         * and any stored data is expected to be in the order of the table.
         * @param columnNames The names of the columns to insert, and an implicit order.
         * @return a reference to this object
         */
        public Builder withColumns(Collection<String> columnNames)
        {
            columns = columnNames;
            return this;
        }

        /**
         * Add the names & order of the columns to be inserted.
         * Order is implied by the order of the ColumnDescriptions in the Collection.
         * <b>NOTE:</b>: As of Riak TS 1.4, this functionality is not implemented server-side,
         * and any stored data is expected to be in the order of the table.
         * @param columns The ColumnDescriptions that contain a column name and an implicit order.
         * @return a reference to this object
         */
        public Builder withColumnDescriptions(Collection<? extends ColumnDescription> columns)
        {
            columns = new ArrayList<>(columns.size());

            for (ColumnDescription column : columns)
            {
                this.columns.add(column.getName());
            }

            return this;
        }

        public Builder withRows(Collection<Row> rows)
        {
            this.rows = rows;
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

        public Collection<String> getColumns()
        {
            return columns;
        }

        public StoreOperation build()
        {
            return new StoreOperation(this);
        }
    }
}
