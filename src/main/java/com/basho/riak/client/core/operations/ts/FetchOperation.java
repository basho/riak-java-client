package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.core.operations.TTBFutureOperation;
import com.basho.riak.client.core.query.timeseries.*;
import com.basho.riak.protobuf.RiakTsPB;
import com.basho.riak.protobuf.RiakMessageCodes;

import java.util.List;

/**
 * An operation to fetch a single row in a Riak Time Series table.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public class FetchOperation extends TTBFutureOperation<QueryResult, String>
{
    private final Builder builder;
    private String queryInfoMessage;

    private FetchOperation(Builder builder)
    {
        super(new TTBConverters.FetchEncoder(builder), new TTBConverters.QueryResultDecoder());
        this.builder = builder;
    }

    @Override
    protected QueryResult convert(List<byte[]> responses)
    {
        // This is not a streaming op, there will only be one response
        final byte[] bytes = checkAndGetSingleResponse(responses);
        return this.responseParser.parseFrom(bytes);
    }

    @Override
    public String getQueryInfo()
    {
        if (this.queryInfoMessage == null)
        {
            this.queryInfoMessage = createQueryInfoMessage();
        }

        return this.queryInfoMessage;
    }

    private String createQueryInfoMessage()
    {
        final StringBuilder sb = new StringBuilder();

        for (Cell cell: this.builder.keyValues)
        {
            if (sb.length() > 0)
            {
                sb.append(", ");
            }
            sb.append( cell == null ? "NULL" : cell.toString());
        }

        return String.format("SELECT * FROM %s WHERE PRIMARY KEY = { %s }",
                this.builder.tableName, sb.toString());
    }

    public static class Builder
    {
        private final String tableName;
        private final Iterable<Cell> keyValues;
        private int timeout;

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

        public FetchOperation build()
        {
            return new FetchOperation(this);
        }

        public String getTableName()
        {
            return tableName;
        }

        public Iterable<Cell> getKeyValues()
        {
            return keyValues;
        }

        public int getTimeout()
        {
            return timeout;
        }
    }
}
