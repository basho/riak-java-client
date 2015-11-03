package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.core.converters.TimeSeriesPBConverter;
import com.basho.riak.client.core.operations.PBFutureOperation;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.google.protobuf.ByteString;

import java.util.List;

/**
 * An operation to fetch a single row in a Riak Time Series table.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class FetchOperation extends PBFutureOperation<QueryResult, RiakKvPB.TsGetResp, BinaryValue>
{
    private final Builder builder;
    private BinaryValue queryInfoMessage;

    private FetchOperation(Builder builder)
    {
        super(RiakMessageCodes.MSG_TsGetReq,
              RiakMessageCodes.MSG_TsGetResp,
              builder.reqBuilder,
              RiakKvPB.TsGetResp.PARSER);

        this.builder = builder;
    }

    @Override
    protected QueryResult convert(List<RiakKvPB.TsGetResp> responses)
    {
        // This is not a streaming op, there will only be one response
        checkIfMoreThanOneResponse(responses);

        final RiakKvPB.TsGetResp response = responses.get(0);

        return TimeSeriesPBConverter.convertPbGetResp(response);
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
        StringBuilder sb = new StringBuilder("SELECT * FROM ");
        sb.append(this.builder.tableName);
        sb.append(" WHERE PRIMARY KEY = { ");

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

        sb.append(" }");

        return BinaryValue.create(sb.toString());
    }

    public static class Builder
    {
        private final BinaryValue tableName;
        private final List<Cell> keyValues;

        private final RiakKvPB.TsGetReq.Builder reqBuilder = RiakKvPB.TsGetReq.newBuilder();

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
            this.reqBuilder.setTimeout(timeout);
            return this;
        }

        public FetchOperation build()
        {
            return new FetchOperation(this);
        }
    }
}
