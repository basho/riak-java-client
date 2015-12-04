package com.basho.riak.client.api.commands.timeseries;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.ts.FetchOperation;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.IQueryResult;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.util.BinaryValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Time Series Fetch Command
 * Allows you to fetch a single time series row by its key values.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class Fetch extends RiakCommand<IQueryResult, BinaryValue>
{
    private static final Logger logger = LoggerFactory.getLogger(Fetch.class);
    private final Builder builder;

    private Fetch(Builder builder)
    {
        this.builder = builder;
    }

    @Override
    protected RiakFuture<IQueryResult, BinaryValue> executeAsync(RiakCluster cluster)
    {
        RiakFuture<IQueryResult, BinaryValue> future =
                cluster.execute(buildCoreOperation());

        return future;
    }

    private FetchOperation buildCoreOperation()
    {
        final FetchOperation.Builder opBuilder =
                new FetchOperation.Builder(BinaryValue.create(this.builder.tableName), builder.keyValues);

        if (builder.timeout > 0)
        {
            opBuilder.withTimeout(builder.timeout);
        }

        return opBuilder.build();
    }

    /**
     * Used to construct a Time Series Fetch command.
     */
    public static class Builder
    {
        private final String tableName;
        private final List<Cell> keyValues;
        private int timeout;

        /**
         * Construct a Builder for a Time Series Fetch command.
         * @param tableName Required. The name of the table to fetch from.
         * @param keyValues Required. The cells that make up the key that identifies which row to fetch.
         *                  Must be in the same order as the table definition.
         */
        public Builder(String tableName, List<Cell> keyValues)
        {
            if (tableName == null || tableName.length() == 0)
            {
                throw new IllegalArgumentException("Table Name cannot be null or an empty string.");
            }

            if (keyValues == null || keyValues.size() == 0)
            {
                throw new IllegalArgumentException("Key Values cannot be null or an empty list.");
            }

            this.tableName = tableName;
            this.keyValues = keyValues;
        }

        /**
         * Set the Riak-side timeout value.
         *
         * @param timeout The timeout, in milliseconds.
         * @return a reference to this object.
         */
        public Builder withTimeout(int timeout)
        {
            if (timeout < 0)
            {
                throw new IllegalArgumentException("Timeout must be positive, or 0 for no timeout.");
            }
            this.timeout = timeout;
            return this;
        }

        /**
         * Construct a Time Series Fetch object.
         * @return a new Time Series Fetch instance.
         */
        public Fetch build()
        {
            return new Fetch(this);
        }
    }
}
