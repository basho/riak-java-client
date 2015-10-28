package com.basho.riak.client.api.commands.timeseries;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.api.cap.VClock;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.ts.DeleteOperation;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakKvPB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Time Series Delete Command
 * Allows you to delete a single time series entry by its key values.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class Delete extends RiakCommand<Void, BinaryValue>
{
    private static final Logger logger = LoggerFactory.getLogger(Delete.class);
    private final Builder builder;

    private Delete(Builder builder)
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

    private DeleteOperation buildCoreOperation()
    {
        final DeleteOperation.Builder opBuilder =
                new DeleteOperation.Builder(BinaryValue.create(this.builder.tableName), builder.keyValues);

        if(builder.timeout > 0)
        {
            opBuilder.withTimeout(builder.timeout);
        }

        return opBuilder.build();
    }

    /**
     * Used to construct a Time Series Delete command.
     */
    public static class Builder
    {
        private final String tableName;
        private final List<Cell> keyValues;
        private int timeout;

        /**
         * Construct a Builder for a Time Series Delete command.
         * @param tableName Required. The name of the table to delete from.
         * @param keyValues Required. The cells that make up the key that identifies which row to delete.
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
            if(timeout < 0)
            {
                throw new IllegalArgumentException("Timeout must be positive, or 0 for no timeout.");
            }
            this.timeout = timeout;
            return this;
        }

        /**
         * Construct a Time Series Delete object.
         * @return a new Time Series Delete instance.
         */
        public Delete build()
        {
            return new Delete(this);
        }
    }
}
