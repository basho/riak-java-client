package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.core.operations.TTBFutureOperation;
import java.util.List;

import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.protobuf.RiakTsPB;
import com.google.protobuf.ByteString;

/**
 * An operation to query data from a Riak Time Series table.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public class QueryOperation extends TTBFutureOperation<QueryResult, String>
{
    private final String queryText;

    private QueryOperation(Builder builder)
    {
        super(new TTBConverters.QueryEncoder(builder), new TTBConverters.QueryResultDecoder());
        this.queryText = builder.queryText;
    }

    @Override
    protected QueryResult convert(List<byte[]> responses)
    {
        // This is not a streaming op, there will only be one response
        final byte[] response = checkAndGetSingleResponse(responses);
        return this.responseParser.parseFrom(response);
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

        public String getQueryText()
        {
            return queryText;
        }

        public QueryOperation build()
        {
            return new QueryOperation(this);
        }
    }
}
