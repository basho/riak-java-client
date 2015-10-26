package com.basho.riak.client.api.commands.timeseries;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.ts.StoreOperation;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.client.core.util.BinaryValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Time Series Store Command
 * Allows you to store data into a Time Series table.
 * Each row to be stored must have it's cells ordered the same as the table definition.
 * To view the table definition, execute <pre>riak-admin bucket-type status <bucket-type-of-TimeSeries-table></pre>
 * on any Riak node in the cluster.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class Store extends RiakCommand<Void,BinaryValue>
{
    private static final Logger logger = LoggerFactory.getLogger(Store.class);

    private final Builder builder;

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

    private StoreOperation buildCoreOperation()
    {
        return new StoreOperation.Builder(BinaryValue.create(builder.tableName.unsafeGetValue()))
                .withRows(builder.rows)
                .build();
    }

    public static class Builder
    {
        private final BinaryValue tableName;
        private final List<Row> rows = new LinkedList<Row>();

        public Builder(BinaryValue tableName)
        {
            this.tableName = tableName;
        }

        public Builder(String tableName)
        {
            this.tableName = BinaryValue.createFromUtf8(tableName);
        }

        public Builder withRow(Row row)
        {
            this.rows.add(row);
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
