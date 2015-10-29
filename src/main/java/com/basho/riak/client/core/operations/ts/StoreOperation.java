package com.basho.riak.client.core.operations.ts;

import com.basho.riak.client.core.converters.TimeSeriesPBConverter;
import com.basho.riak.client.core.operations.PBFutureOperation;
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
public class StoreOperation
        extends PBFutureOperation<Void, RiakKvPB.TsPutResp, BinaryValue, RiakKvPB.TsPutReq.Builder>
{
    private final Logger logger = LoggerFactory.getLogger(StoreOperation.class);

    private StoreOperation(Builder builder)
    {
        super(RiakMessageCodes.MSG_TsPutReq,
              RiakMessageCodes.MSG_TsPutResp,
              builder.reqBuilder,
              RiakKvPB.TsPutResp.PARSER);
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
            this.reqBuilder.addAllColumns(TimeSeriesPBConverter.convertColumnDescriptionsToPb(columns));
            return this;
        }

        public Builder withRows(Collection<Row> rows)
        {
            this.reqBuilder.addAllRows(TimeSeriesPBConverter.convertRowsToPb(rows));
            return this;
        }

        public StoreOperation build()
        {
            return new StoreOperation(this);
        }
    }
}


