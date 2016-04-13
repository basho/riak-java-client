package com.basho.riak.client.core.operations.ts;

import java.util.List;

import com.basho.riak.client.core.operations.PBFutureOperation;
import com.basho.riak.client.core.query.timeseries.PbResultFactory;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakTsPB;
import com.google.protobuf.ByteString;

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
              builder.reqBuilder,
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
        private final RiakTsPB.TsQueryReq.Builder reqBuilder = RiakTsPB.TsQueryReq.newBuilder();

        public Builder(String queryText)
        {
            if (queryText == null || queryText.length() == 0)
            {
                throw new IllegalArgumentException("QueryText cannot be null or empty");
            }
            this.queryText = queryText;
            RiakTsPB.TsInterpolation.Builder interpolationBuilder = RiakTsPB.TsInterpolation.newBuilder().setBase(ByteString.copyFromUtf8(queryText));
            reqBuilder.setQuery(interpolationBuilder);
        }

        public Builder withCoverageContext(byte[] coverageContext) {
            if(coverageContext != null) {
                reqBuilder.setCoverContext(ByteString.copyFrom(coverageContext));
            }
            return this;
        }

        public QueryOperation build()
        {
            return new QueryOperation(this);
        }
    }
}
