package com.basho.riak.client.api.commands.timeseries;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.api.commands.CoreFutureAdapter;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.ts.ListKeysOperation;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.client.core.util.BinaryValue;

import java.util.List;

/**
 * Time Series List Keys Command
 * Allows you to List the Primary Keys in a Time Series Table.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class ListKeys extends RiakCommand<ListKeys.Response, BinaryValue>
{
    private final BinaryValue tableName;
    private final int timeout;

    private ListKeys(ListKeys.Builder builder)
    {
        this.tableName = builder.tableName;
        this.timeout = builder.timeout;
    }

    @Override
    protected RiakFuture<Response, BinaryValue> executeAsync(RiakCluster cluster)
    {
        RiakFuture<ListKeysOperation.Response, BinaryValue> coreFuture =
                cluster.execute(buildCoreOperation());

        CoreFutureAdapter<Response, BinaryValue, ListKeysOperation.Response, BinaryValue> future =
                new CoreFutureAdapter<ListKeys.Response, BinaryValue, ListKeysOperation.Response, BinaryValue>(coreFuture)
                {
                    @Override
                    protected Response convertResponse(ListKeysOperation.Response coreResponse)
                    {
                        return new Response(tableName, coreResponse.getRows());
                    }

                    @Override
                    protected BinaryValue convertQueryInfo(BinaryValue coreQueryInfo)
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

    public class Response {
        private final BinaryValue tableName;
        private final List<Row> rows;

        public Response(BinaryValue tableName, List<Row> rows)
        {
            this.tableName = tableName;
            this.rows = rows;
        }
    }

    public class Builder
    {
        private final BinaryValue tableName;
        private int timeout;

        public Builder(String tableName)
        {
            this.tableName = BinaryValue.createFromUtf8(tableName);
        }

        public Builder(BinaryValue tableName)
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

        public ListKeys build()
        {
            return new ListKeys(this);
        }


    }
}
