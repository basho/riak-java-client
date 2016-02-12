package com.basho.riak.client.api.commands.timeseries;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.api.commands.CoreFutureAdapter;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
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
public class ListKeys extends RiakCommand<QueryResult, String>
{
    private final String tableName;
    private final int timeout;

    private ListKeys(ListKeys.Builder builder)
    {
        this.tableName = builder.tableName;
        this.timeout = builder.timeout;
    }

    @Override
    protected RiakFuture<QueryResult, String> executeAsync(RiakCluster cluster)
    {
        RiakFuture<QueryResult, String> coreFuture =
                cluster.execute(buildCoreOperation());

        CoreFutureAdapter<QueryResult, String, QueryResult, String> future =
                new CoreFutureAdapter<QueryResult, String, QueryResult, String>(coreFuture)
                {
                    @Override
                    protected QueryResult convertResponse(QueryResult coreResponse)
                    {
                        return coreResponse;
                    }

                    @Override
                    protected String convertQueryInfo(String coreQueryInfo)
                    {
                        return coreQueryInfo;
                    }
                };
        coreFuture.addListener(future);
        return future;
    }

    private ListKeysOperation buildCoreOperation()
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
         * By default, riak has a 60s timeout for operations. Setting
         * this value will override that default for this operation.
         * </p>
         * @param timeout the timeout in milliseconds to be sent to riak.
         * @return a reference to this object.
         */
        public Builder withTimeout(int timeout)
        {
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
