package com.basho.riak.client.api.commands.timeseries;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.ts.DescribeTableOperation;
import com.basho.riak.client.core.query.timeseries.TableDefinition;

/**
 * Time Series DescribeTable Command
 * Allows you to fetch a table definition from Riak.
 *
 * @author Alex Moore <amoore at basho dot com>
  * @since 2.0.4
 */
public class DescribeTable extends RiakCommand<TableDefinition, String>
{
    private final String tableName;

    public DescribeTable(String tableName)
    {
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
