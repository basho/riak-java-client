package com.basho.riak.client.core.operations;

import com.basho.riak.client.core.converters.TimeSeriesConverter;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakPB;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class TimeSeriesQueryOperation extends PBFutureOperation<QueryResult, RiakKvPB.TsQueryResp, BinaryValue> {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesQueryOperation.class);

    private TimeSeriesQueryOperation(Builder builder)
    {
        super(RiakMessageCodes.MSG_TsQueryReq, RiakMessageCodes.MSG_TsQueryResp,
                RiakKvPB.TsQueryReq.newBuilder().setQuery(builder.interpolationBuilder), RiakKvPB.TsQueryResp.PARSER);
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


        TimeSeriesConverter converter = new TimeSeriesConverter();
        QueryResult result = converter.convert(response);

        return result;
    }

    @Override
    public BinaryValue getQueryInfo() {
        return null;
    }


    public static class Builder
    {
        private final RiakKvPB.TsInterpolation.Builder interpolationBuilder =
                RiakKvPB.TsInterpolation.newBuilder();

        public Builder(BinaryValue queryText)
        {
            if (queryText == null || queryText.length() == 0)
            {
                throw new IllegalArgumentException("QueryText can not be null or empty");
            }

            this.interpolationBuilder.setBase(ByteString.copyFrom(queryText.unsafeGetValue()));
        }

        private void addInterpolationAt(int index, BinaryValue key, BinaryValue value)
        {
            ByteString bsKey = ByteString.copyFrom(key.unsafeGetValue());
            ByteString bsValue = ByteString.copyFrom(value.unsafeGetValue());

            RiakPB.RpbPair.Builder pair = RiakPB.RpbPair.newBuilder().setKey(bsKey).setValue(bsValue);
            this.interpolationBuilder.setInterpolations(index, pair);
        }

        public Builder setInterpolations(Map<BinaryValue, BinaryValue> interpolations)
        {
            int i = this.interpolationBuilder.getInterpolationsCount();

            for (Map.Entry<BinaryValue, BinaryValue> interpolation : interpolations.entrySet())
            {
                addInterpolationAt(i, interpolation.getKey(), interpolation.getValue());
                i++;
            }

            return this;
        }

        public TimeSeriesQueryOperation build()
        {
            return new TimeSeriesQueryOperation(this);
        }
    }
}
