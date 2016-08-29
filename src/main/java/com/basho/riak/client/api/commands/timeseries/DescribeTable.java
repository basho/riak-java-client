package com.basho.riak.client.api.commands.timeseries;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.ts.DescribeTableOperation;
import com.basho.riak.client.core.query.timeseries.TableDefinition;

/**
 * Time Series DescribeTable Command
 * Allows you to fetch a table definition from Riak Time Series.
 *
 * @author Alex Moore <amoore at basho dot com>
  * @since 2.0.4
 */
public class DescribeTable extends RiakCommand<TableDefinition, String>
{
    private final String tableName;

    /**
     * Create a new DescribeTable command.
     * No Builder is required.
     *
     * @param tableName The name of the table to fetch a definition for. Required, must not be empty or null.
     */
    public DescribeTable(String tableName)
    {
        if (tableName == null || tableName.isEmpty())
        {
            throw new IllegalArgumentException("Table Name must not be null or empty.");
        }

        this.tableName = tableName;
    }

    @Override
    protected RiakFuture<TableDefinition, String> executeAsync(RiakCluster cluster)
    {
        RiakFuture<TableDefinition, String> future =
                cluster.execute(buildCoreOperation());

        return future;
    }

    private DescribeTableOperation buildCoreOperation()
    {
        return new DescribeTableOperation(this.tableName);
    }
}
