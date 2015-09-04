package com.basho.riak.client.core.operations;

import com.basho.riak.client.core.converters.TimeSeriesPBConverter;
import com.basho.riak.client.core.query.timeseries.ColumnDescription;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

/**
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class TimeSeriesStoreOperation  extends PBFutureOperation<Void, RiakKvPB.TsPutResp, BinaryValue>
{
    private final Logger logger = LoggerFactory.getLogger(TimeSeriesStoreOperation.class);

    private final RiakKvPB.TsPutReq.Builder reqBuilder;

    private TimeSeriesStoreOperation(Builder builder)
    {
        super(RiakMessageCodes.MSG_TsPutReq, RiakMessageCodes.MSG_TsPutResp, builder.reqBuilder, RiakKvPB.TsPutResp.PARSER);
        this.reqBuilder = builder.reqBuilder;
    }

    @Override
    protected Void convert(List<RiakKvPB.TsPutResp> responses) {

        // This is not a streaming op, there will only be one response
        if (responses.size() > 1)
        {
            logger.error("Received {} responses when only one was expected.", responses.size());
        }

        return null;
    }

    @Override
    public BinaryValue getQueryInfo() {
        return null;
    }

    public static class Builder
    {
        private final RiakKvPB.TsPutReq.Builder reqBuilder;

        public Builder(BinaryValue tableName)
        {
            if (tableName == null || tableName.length() == 0)
            {
                throw new IllegalArgumentException("TableName can not be null or empty");
            }

            this.reqBuilder = RiakKvPB.TsPutReq.newBuilder();
            this.reqBuilder.setTable(ByteString.copyFrom(tableName.unsafeGetValue()));
        }

        public Builder withColumns(Collection<ColumnDescription> columns)
        {
            this.reqBuilder.addAllColumns(TimeSeriesPBConverter.convertColumns(columns));
            return this;
        }

        public Builder withRows(Collection<Row> rows)
        {
            this.reqBuilder.addAllRows(TimeSeriesPBConverter.convertRows(rows));
            return this;
        }

        public TimeSeriesStoreOperation build()
        {
            return new TimeSeriesStoreOperation(this);
        }
    }
}


