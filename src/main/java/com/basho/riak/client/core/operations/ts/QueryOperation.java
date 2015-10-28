package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.core.converters.TimeSeriesPBConverter;
import com.basho.riak.client.core.operations.PBFutureOperation;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * An operation to query data from a Riak Time Series table.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class QueryOperation extends PBFutureOperation<QueryResult, RiakKvPB.TsQueryResp, BinaryValue>
{
    private final BinaryValue queryText;
    private static final Logger logger = LoggerFactory.getLogger(QueryOperation.class);

    private QueryOperation(Builder builder)
    {
        super(RiakMessageCodes.MSG_TsQueryReq,
              RiakMessageCodes.MSG_TsQueryResp,
              RiakKvPB.TsQueryReq.newBuilder().setQuery(builder.interpolationBuilder),
              RiakKvPB.TsQueryResp.PARSER);

        this.queryText = builder.queryText;
    }

    @Override
    protected QueryResult convert(List<RiakKvPB.TsQueryResp> responses)
    {
        // This is not a streaming op, there will only be one response
        if (responses.size() > 1)
        {
            logger.error("Received {} responses when only one was expected.", responses.size());
        }

        RiakKvPB.TsQueryResp response = responses.get(0);

        QueryResult result = TimeSeriesPBConverter.convertPbGetResp(response);

        return result;
    }

    @Override
    public BinaryValue getQueryInfo() {
        return queryText;
    }


    public static class Builder
    {
        private final BinaryValue queryText;
        private final RiakKvPB.TsInterpolation.Builder interpolationBuilder =
                RiakKvPB.TsInterpolation.newBuilder();

        public Builder(BinaryValue queryText)
        {
            if (queryText == null || queryText.length() == 0)
            {
                throw new IllegalArgumentException("QueryText cannot be null or empty");
            }
            this.queryText = queryText;
            this.interpolationBuilder.setBase(ByteString.copyFrom(queryText.unsafeGetValue()));
        }

//        private void addInterpolationAt(int index, BinaryValue key, BinaryValue value)
//        {
//            ByteString bsKey = ByteString.copyFrom(key.unsafeGetValue());
//            ByteString bsValue = ByteString.copyFrom(value.unsafeGetValue());
//
//            RiakPB.RpbPair.Builder pair = RiakPB.RpbPair.newBuilder().setKey(bsKey).setValue(bsValue);
//            this.interpolationBuilder.setInterpolations(index, pair);
//        }
//
//        public Builder setInterpolations(Map<BinaryValue, BinaryValue> interpolations)
//        {
//            int i = this.interpolationBuilder.getInterpolationsCount();
//
//            for (Map.Entry<BinaryValue, BinaryValue> interpolation : interpolations.entrySet())
//            {
//                addInterpolationAt(i, interpolation.getKey(), interpolation.getValue());
//                i++;
//            }
//
//            return this;
//        }

        public QueryOperation build()
        {
            return new QueryOperation(this);
        }
    }
}
