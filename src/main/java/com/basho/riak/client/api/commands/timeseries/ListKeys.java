package com.basho.riak.client.api.commands.timeseries;

import com.basho.riak.client.api.AsIsRiakCommand;
import com.basho.riak.client.core.operations.ts.ListKeysOperation;
import com.basho.riak.client.core.query.timeseries.QueryResult;

/**
 * Time Series List Keys Command
 * Allows you to List the Primary Keys in a Time Series Table.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public class ListKeys extends AsIsRiakCommand<QueryResult, String>
{
    private final String tableName;
    private final int timeout;

    private ListKeys(ListKeys.Builder builder)
    {
        this.tableName = builder.tableName;
        this.timeout = builder.timeout;
    }

    @Override
    protected ListKeysOperation buildCoreOperation()
    {
        ListKeysOperation.Builder builder = new ListKeysOperation.Builder(tableName);

        if (this.timeout > 0)
        {
            builder.withTimeout(this.timeout);
        }

        return builder.build();
    }

    /**
     * Used to construct a Time Series ListKeys command.
     */
    public static class Builder
    {
        private final String tableName;
        private int timeout;

        /**
         * Construct a Builder for a Time Series ListKeys command.
         * @param tableName Required. The name of the table to list keys from.
         */
        public Builder(String tableName)
        {
            this.tableName = tableName;
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
         * Construct a Time Series ListKeys object.
         * @return a new Time Series ListKeys instance.
         */
        public ListKeys build()
        {
            return new ListKeys(this);
        }
    }
}
