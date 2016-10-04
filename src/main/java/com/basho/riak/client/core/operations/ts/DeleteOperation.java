package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.core.operations.PBFutureOperation;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.ConvertibleIterable;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakTsPB;
import com.google.protobuf.ByteString;

import java.util.List;

/**
 * An operation to delete a row in a Riak Time Series table.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public class DeleteOperation extends PBFutureOperation<Void, RiakTsPB.TsDelResp, String>
{
    private final Builder builder;
    private String queryInfoMessage;

    private DeleteOperation(Builder builder)
    {
        super(RiakMessageCodes.MSG_TsDelReq,
              RiakMessageCodes.MSG_TsDelResp,
              builder.reqBuilder,
              RiakTsPB.TsDelResp.PARSER);

        this.builder = builder;
    }

    @Override
    protected Void convert(List<RiakTsPB.TsDelResp> responses)
    {
        // This is not a streaming op, there will only be one response
        checkAndGetSingleResponse(responses);

        return null;
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

        return String.format("DELETE { %s } FROM TABLE %s",
                sb.toString(), this.builder.tableName);
    }

    public static class Builder
    {
        private final String tableName;
        private final Iterable<Cell> keyValues;

        private final RiakTsPB.TsDelReq.Builder reqBuilder = RiakTsPB.TsDelReq.newBuilder();

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

            this.reqBuilder.setTable(ByteString.copyFromUtf8(tableName));
            this.reqBuilder.addAllKey(ConvertibleIterable.asIterablePbCell(keyValues));

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

            reqBuilder.setTimeout(timeout);
            return this;
        }

        public DeleteOperation build()
        {
            return new DeleteOperation(this);
        }
    }
}
