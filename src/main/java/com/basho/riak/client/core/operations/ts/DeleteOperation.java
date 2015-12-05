package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.core.operations.PBFutureOperation;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.CollectionConverters;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakTsPB;
import com.google.protobuf.ByteString;

import java.util.Collection;
import java.util.List;

/**
 * An operation to delete a row in a Riak Time Series table.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public class DeleteOperation extends PBFutureOperation<Void, RiakTsPB.TsDelResp, BinaryValue>
{
    private final Builder builder;
    private BinaryValue queryInfoMessage;

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
    public BinaryValue getQueryInfo()
    {
        if (this.queryInfoMessage == null)
        {
            this.queryInfoMessage = createQueryInfoMessage();
        }

        return this.queryInfoMessage;
    }

    private BinaryValue createQueryInfoMessage()
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

        return BinaryValue.create(
                String.format("DELETE { %s } FROM TABLE %s",
                        sb.toString(), this.builder.tableName)
        );

    }

    public static class Builder
    {
        private final BinaryValue tableName;
        private final Collection<Cell> keyValues;

        private final RiakTsPB.TsDelReq.Builder reqBuilder = RiakTsPB.TsDelReq.newBuilder();

        public Builder(BinaryValue tableName, Collection<Cell> keyValues)
        {
            if (tableName == null || tableName.length() == 0)
            {
                throw new IllegalArgumentException("Table Name cannot be null or an empty string.");
            }

            if (keyValues == null || keyValues.size() == 0)
            {
                throw new IllegalArgumentException("Key Values cannot be null or an empty list.");
            }

            this.reqBuilder.setTable(ByteString.copyFrom(tableName.getValue()));
            this.reqBuilder.addAllKey(CollectionConverters.wrapAsIterablePBCell(keyValues.iterator()));

            this.tableName = tableName;
            this.keyValues = keyValues;
        }

        public Builder withTimeout(int timeout)
        {
            if (timeout < 0)
            {
                throw new IllegalArgumentException("Timeout must be positive, or 0 for no timeout.");
            }
            this.reqBuilder.setTimeout(timeout);
            return this;
        }

        public DeleteOperation build()
        {
            return new DeleteOperation(this);
        }
    }
}
