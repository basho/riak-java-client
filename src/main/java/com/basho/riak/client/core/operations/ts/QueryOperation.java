package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.core.operations.PBFutureOperation;
import com.basho.riak.client.core.query.timeseries.PbResultFactory;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakTsPB;
import com.google.protobuf.ByteString;

import java.util.List;

/**
 * An operation to query data from a Riak Time Series table.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public class QueryOperation extends PBFutureOperation<QueryResult, RiakTsPB.TsQueryResp, String>
{
    private final String queryText;

    private QueryOperation(Builder builder)
    {
        super(RiakMessageCodes.MSG_TsQueryReq,
              RiakMessageCodes.MSG_TsQueryResp,
              RiakTsPB.TsQueryReq.newBuilder().setQuery(builder.interpolationBuilder),
              RiakTsPB.TsQueryResp.PARSER);

        this.queryText = builder.queryText;
    }

    @Override
    protected QueryResult convert(List<RiakTsPB.TsQueryResp> responses)
    {
        // This is not a streaming op, there will only be one response
        final RiakTsPB.TsQueryResp response = checkAndGetSingleResponse(responses);
        return PbResultFactory.convertPbQueryResp(response);
    }

    @Override
    public String getQueryInfo()
    {
        return this.queryText;
    }

    public static class Builder
    {
        private final String queryText;
        private final RiakTsPB.TsInterpolation.Builder interpolationBuilder = RiakTsPB.TsInterpolation.newBuilder();

        public Builder(String queryText)
        {
            if (queryText == null || queryText.length() == 0)
            {
                throw new IllegalArgumentException("QueryText cannot be null or empty");
            }
            this.queryText = queryText;
            this.interpolationBuilder.setBase(ByteString.copyFromUtf8(queryText));
        }

        public QueryOperation build()
        {
            return new QueryOperation(this);
        }
    }
}
