package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.core.operations.PBFutureOperation;
import com.basho.riak.client.core.query.timeseries.PbResultFactory;
import com.basho.riak.client.core.query.timeseries.TableDefinition;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakTsPB;
import com.google.protobuf.ByteString;

import java.util.List;

/**
 * An operation to query a table definition from Riak Time Series.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.4
 */
public class DescribeTableOperation extends PBFutureOperation<TableDefinition, RiakTsPB.TsQueryResp, String>
{
    private final String tableName;
    private final String queryText;

    public DescribeTableOperation(String tableName)
    {
        this(new Builder(tableName));
    }

    private DescribeTableOperation(Builder builder)
    {
        super(RiakMessageCodes.MSG_TsQueryReq,
              RiakMessageCodes.MSG_TsQueryResp,
              RiakTsPB.TsQueryReq.newBuilder().setQuery(builder.interpolationBuilder),
              RiakTsPB.TsQueryResp.PARSER);

        this.queryText = builder.queryText;
        this.tableName = builder.tableName;
    }

    @Override
    protected TableDefinition convert(List<RiakTsPB.TsQueryResp> responses)
    {
        // This is not a streaming op, there will only be one response
        final RiakTsPB.TsQueryResp response = checkAndGetSingleResponse(responses);
        return PbResultFactory.convertDescribeResp(this.tableName, response);
    }

    @Override
    public String getQueryInfo()
    {
        return this.queryText;
    }

    public static class Builder
    {
        private final String tableName;
        private final String queryText;
        private final RiakTsPB.TsInterpolation.Builder interpolationBuilder = RiakTsPB.TsInterpolation.newBuilder();

        public Builder(String tableName)
        {
            if (tableName == null || tableName.length() == 0)
            {
                throw new IllegalArgumentException("Table Name cannot be null or empty");
            }

            this.tableName = tableName;
            this.queryText = String.format("DESCRIBE %s", tableName);
            this.interpolationBuilder.setBase(ByteString.copyFromUtf8(queryText));
        }

        public DescribeTableOperation build()
        {
            return new DescribeTableOperation(this);
        }
    }
}
