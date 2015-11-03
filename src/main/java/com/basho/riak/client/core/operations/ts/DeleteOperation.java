package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.core.converters.TimeSeriesPBConverter;
import com.basho.riak.client.core.operations.PBFutureOperation;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.google.protobuf.ByteString;

import java.util.List;

/**
 * An operation to delete a row in a Riak Time Series table.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class DeleteOperation extends PBFutureOperation<Void, RiakKvPB.TsDelResp, BinaryValue>
{
    private final Builder builder;
    private BinaryValue queryInfoMessage;

    private DeleteOperation(Builder builder)
    {
        super(RiakMessageCodes.MSG_TsDelReq,
              RiakMessageCodes.MSG_TsDelResp,
              builder.reqBuilder,
              RiakKvPB.TsDelResp.PARSER);

        this.builder = builder;
    }

    @Override
    protected Void convert(List<RiakKvPB.TsDelResp> responses)
    {
        // This is not a streaming op, there will only be one response
        int numResponses = responses.size();
        checkIfMoreThanOneResponse(responses);

        return null;
    }

    @Override
    public synchronized BinaryValue getQueryInfo()
    {
        if (this.queryInfoMessage == null)
        {
            this.queryInfoMessage = createQueryInfoMessage();
        }

        return this.queryInfoMessage;
    }

    private BinaryValue createQueryInfoMessage()
    {
        StringBuilder sb = new StringBuilder("DELETE ");
        sb.append("{ ");

        int numKeys = this.builder.keyValues.size();
        for (int i = 0; i < numKeys; i++)
        {
            if (this.builder.keyValues.get(i) == null)
            {
                sb.append("NULL");
            }
            else
            {
                sb.append(this.builder.keyValues.get(i).toString());
            }

            if (i < numKeys-1)
            {
                sb.append(", ");
            }
        }

        sb.append(" } FROM TABLE ");
        sb.append(this.builder.tableName.toStringUtf8());
        return BinaryValue.create(sb.toString());
    }

    public static class Builder
    {
        private final BinaryValue tableName;
        private final List<Cell> keyValues;

        private final RiakKvPB.TsDelReq.Builder reqBuilder = RiakKvPB.TsDelReq.newBuilder();

        public Builder(BinaryValue tableName, List<Cell> keyValues)
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
            this.reqBuilder.addAllKey(TimeSeriesPBConverter.convertCellsToPb(keyValues));

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
