package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.core.converters.TimeSeriesPBConverter;
import com.basho.riak.client.core.operations.PBFutureOperation;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by alex on 10/26/15.
 */
public class FetchOperation
        extends PBFutureOperation<QueryResult, RiakKvPB.TsGetResp, BinaryValue, RiakKvPB.TsGetReq.Builder>
{
    private static final Logger logger = LoggerFactory.getLogger(FetchOperation.class);

    private FetchOperation(Builder builder)
    {
        super(RiakMessageCodes.MSG_TsGetReq,
              RiakMessageCodes.MSG_TsGetResp,
              RiakKvPB.TsGetReq.newBuilder(),
              RiakKvPB.TsGetResp.PARSER);

        this.reqBuilder.setTable(ByteString.copyFrom(builder.tableName.getValue()));
        this.reqBuilder.addAllKey(TimeSeriesPBConverter.convertCellsToPb(builder.keyValues));

        if(builder.timeout != 0)
        {
            this.reqBuilder.setTimeout(builder.timeout);
        }
    }

    @Override
    protected QueryResult convert(List<RiakKvPB.TsGetResp> responses)
    {
        // This is not a streaming op, there will only be one response
        if (responses.size() > 1)
        {
            logger.error("Received {} responses when only one was expected.", responses.size());
        }

        final RiakKvPB.TsGetResp response = responses.get(0);


        QueryResult result = TimeSeriesPBConverter.convertPbGetResp(response);

        return result;
    }

    @Override
    public BinaryValue getQueryInfo()
    {
        return null;
    }

    public static class Builder
    {
        private final BinaryValue tableName;
        private final ArrayList<Cell> keyValues;
        private int timeout = 0;

        public Builder(String tableName, Collection<Cell> keyValues)
        {
            this.tableName = BinaryValue.createFromUtf8(tableName);
            this.keyValues = new ArrayList<Cell>(keyValues);
        }

        public Builder withTimeout(int timeout)
        {
            this.timeout = timeout;
            return this;
        }

        public FetchOperation build()
        {
            return new FetchOperation(this);
        }
    }
}
