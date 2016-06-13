package com.basho.riak.client.api.commands.timeseries;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.ts.StoreOperation;
import com.basho.riak.client.core.query.timeseries.Row;

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
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public class Store extends RiakCommand<Void,String>
{
    private final Builder builder;

    private Store (Builder builder)
    {
        this.builder = builder;
    }

    @Override
    protected RiakFuture<Void, String> executeAsync(RiakCluster cluster)
    {
        RiakFuture<Void, String> future =
                cluster.execute(buildCoreOperation());

        return future;
    }

    private StoreOperation buildCoreOperation()
    {
        return new StoreOperation.Builder(builder.tableName)
                .withRows(builder.rows)
                .build();
    }

    /**
     * Used to construct a Time Series Store command.
     */
    public static class Builder
    {
        private final String tableName;
        // TODO: Think about using a flattening iterable here. 
        private final List<Row> rows = new LinkedList<>();

        /**
         * Construct a Builder for a Time Series Store command.
         * @param tableName Required. The name of the table to store data to.
         */
        public Builder(String tableName)
        {
            this.tableName = tableName;
        }

        /**
         * Add a single Row object to the store command.
         * @param row Required. The row to add.
         * @return a reference to this object.
         */
        public Builder withRow(Row row)
        {
            this.rows.add(row);
            return this;
        }

        /**
         * Add a collection of Row objects to the store command.
         * @param rows Required. The rows to add.
         * @return a reference to this object.
         */
        public Builder withRows(Iterable<Row> rows)
        {
            if (rows instanceof Collection)
            {
                this.rows.addAll((Collection<Row>) rows);
            }
            else
            {
                // A bit weird but have no other ideas
                for (Row r : rows)
                {
                    this.rows.add(r);
                }
            }
            return this;
        }

        /**
         * Construct a Time Series Store object.
         * @return a new Time Series Store instance.
         */
        public Store build()
        {
            return new Store(this);
        }
    }
}
