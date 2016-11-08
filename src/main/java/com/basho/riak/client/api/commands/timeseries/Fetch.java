package com.basho.riak.client.api.commands.timeseries;

import com.basho.riak.client.api.AsIsRiakCommand;
import com.basho.riak.client.core.operations.ts.FetchOperation;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;

/**
 * Time Series Fetch Command
 * Allows you to fetch a single time series row by its key values.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public class Fetch extends AsIsRiakCommand<QueryResult, String>
{
    private final Builder builder;

    private Fetch(Builder builder)
    {
        this.builder = builder;
    }

    @Override
    protected FetchOperation buildCoreOperation()
    {
        final FetchOperation.Builder opBuilder =
                new FetchOperation.Builder(this.builder.tableName, builder.keyValues);

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
        private final Iterable<Cell> keyValues;
        private int timeout;

        /**
         * Construct a Builder for a Time Series Fetch command.
         * @param tableName Required. The name of the table to fetch from.
         * @param keyValues Required. The cells that make up the key that identifies which row to fetch.
         *                  Must be in the same order as the table definition.
         */
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

            this.tableName = tableName;
            this.keyValues = keyValues;
        }

        /**
         * Set the Riak-side timeout value.
         * <p>
         * By default, Riak has a 60s timeout for operations. Setting
         * this value will override that default for this operation.
         * </p>
         * @param timeout the timeout in milliseconds to be sent to riak.
         * @return a reference to this object.
         */
        public Builder withTimeout(int timeout)
        {
            if (timeout < 1)
            {
                throw new IllegalArgumentException("Timeout must be a positive integer");
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
