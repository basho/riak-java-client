package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.core.converters.TimeSeriesPBConverter;
import com.basho.riak.client.core.operations.PBFutureOperation;
import com.basho.riak.client.core.query.timeseries.IQueryResult;
import com.basho.riak.client.core.query.timeseries.immutable.pb.ImmutablePbResultFactory;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakTsPB;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.google.protobuf.ByteString;

import java.util.List;

/**
 * An operation to query data from a Riak Time Series table.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public class QueryOperation extends PBFutureOperation<IQueryResult, RiakTsPB.TsQueryResp, BinaryValue>
{
    private final BinaryValue queryText;
    private final boolean shouldReturnImmutableResults;

    private QueryOperation(Builder builder)
    {
        super(RiakMessageCodes.MSG_TsQueryReq,
              RiakMessageCodes.MSG_TsQueryResp,
              RiakTsPB.TsQueryReq.newBuilder().setQuery(builder.interpolationBuilder),
              RiakTsPB.TsQueryResp.PARSER);

        this.queryText = builder.queryText;
        this.shouldReturnImmutableResults = builder.shouldReturnImmutableResults;
    }

    @Override
    protected IQueryResult convert(List<RiakTsPB.TsQueryResp> responses)
    {
        // This is not a streaming op, there will only be one response
        final RiakTsPB.TsQueryResp response = checkAndGetSingleResponse(responses);

        if (shouldReturnImmutableResults)
        {
            return ImmutablePbResultFactory.convertPbQueryResp(response);
        }
        else
        {
            return TimeSeriesPBConverter.convertPbGetResp(response);
        }
    }
    @Override
    public BinaryValue getQueryInfo()
    {
        return this.queryText;
    }

    public static class Builder
    {
        private final BinaryValue queryText;
        private boolean shouldReturnImmutableResults = RETURN_IMMUTABLE_RESULTS;
        private final RiakTsPB.TsInterpolation.Builder interpolationBuilder =
                RiakTsPB.TsInterpolation.newBuilder();

        public static boolean RETURN_IMMUTABLE_RESULTS = false;

        public Builder(BinaryValue queryText)
        {
            if (queryText == null || queryText.length() == 0)
            {
                throw new IllegalArgumentException("QueryText cannot be null or empty");
            }
            this.queryText = queryText;
            this.interpolationBuilder.setBase(ByteString.copyFrom(queryText.unsafeGetValue()));
        }

        public Builder returnImmutableResults(boolean shouldReturnImmutableResults)
        {
            this.shouldReturnImmutableResults = shouldReturnImmutableResults;
            return this;
        }

        public QueryOperation build()
        {
            return new QueryOperation(this);
        }
    }
}
