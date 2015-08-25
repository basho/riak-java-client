package com.basho.riak.client.api.commands.timeseries;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.TimeSeriesStoreOperation;
import com.basho.riak.client.core.query.timeseries.ColumnDescription;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.client.core.util.BinaryValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by alex on 8/24/15.
 */
public class Store extends RiakCommand<Void,BinaryValue>
{
    private final Builder builder;
    private final Logger logger = LoggerFactory.getLogger(Query.class);
    private Store (Builder builder)
    {
        this.builder = builder;
    }

    @Override
    protected RiakFuture<Void, BinaryValue> executeAsync(RiakCluster cluster)
    {
        RiakFuture<Void, BinaryValue> future =
                cluster.execute(buildCoreOperation());

        return future;
    }

    private TimeSeriesStoreOperation buildCoreOperation()
    {
        return new TimeSeriesStoreOperation.Builder(BinaryValue.create(builder.tableName.unsafeGetValue()))
                .withColumns(builder.columns)
                .withRows(builder.rows)
                .build();
    }

    public static class Builder
    {
        private final BinaryValue tableName;
        private final List<Row> rows = new ArrayList<Row>();
        private final List<ColumnDescription> columns = new ArrayList<ColumnDescription>();

        public Builder(BinaryValue tableName)
        {
            this.tableName = tableName;
        }

        public Builder(String tableName)
        {
            this.tableName = BinaryValue.createFromUtf8(tableName);
        }

        public Builder withColumns(Collection<ColumnDescription> columns)
        {
            this.columns.addAll(columns);
            return this;
        }

        public Builder withRows(Collection<Row> rows)
        {
            this.rows.addAll(rows);
            return this;
        }

        public Store build()
        {
            return new Store(this);
        }
    }
}
