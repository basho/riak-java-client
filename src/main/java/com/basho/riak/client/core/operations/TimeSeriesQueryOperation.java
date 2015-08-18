package com.basho.riak.client.core.operations;

import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.core.converters.TimeSeriesConverter;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class TimeSeriesQueryOperation extends FutureOperation<QueryResult, RiakKvPB.TsQueryResp, BinaryValue> {

    private final RiakKvPB.TsQueryReq.Builder reqBuilder;

    private final Logger logger = LoggerFactory.getLogger(TimeSeriesQueryOperation.class);

    private TimeSeriesQueryOperation(Builder builder)
    {
        this.reqBuilder = RiakKvPB.TsQueryReq.newBuilder().setQuery(builder.interpolationBuilder);
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
    protected RiakMessage createChannelMessage()
    {
        RiakKvPB.TsQueryReq req = reqBuilder.build();
        return new RiakMessage(RiakMessageCodes.MSG_TsQueryReq, req.toByteArray());
    }

    @Override
    protected RiakKvPB.TsQueryResp decode(RiakMessage rawMessage) {
        Operations.checkMessageType(rawMessage, RiakMessageCodes.MSG_TsQueryResp);

        try
        {
            byte[] data = rawMessage.getData();

            if (data.length == 0) // not found
            {
                return null;
            }

            return RiakKvPB.TsQueryResp.parseFrom(data);
        }
        catch (InvalidProtocolBufferException e)
        {
            logger.error("Invalid message received; {}", e);
            throw new IllegalArgumentException("Invalid message received", e);
        }
    }

    @Override
    public BinaryValue getQueryInfo() {
        return null;
    }


    public static class Builder
    {
        private final RiakKvPB.TsInterpolation.Builder interpolationBuilder =
                RiakKvPB.TsInterpolation.newBuilder();

        private final BinaryValue queryText;

        public Builder(BinaryValue queryText)
        {
            if (queryText == null || queryText.length() == 0)
            {
                throw new IllegalArgumentException("QueryText can not be null or empty");
            }

            this.queryText = queryText;

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
