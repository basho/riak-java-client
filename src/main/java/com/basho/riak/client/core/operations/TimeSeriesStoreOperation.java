package com.basho.riak.client.core.operations;

import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.core.converters.TimeSeriesConverter;
import com.basho.riak.client.core.query.timeseries.ColumnDescription;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

/**
 * Created by alex on 8/17/15.
 */
public class TimeSeriesStoreOperation  extends FutureOperation<Void, RiakKvPB.TsPutResp, BinaryValue>
{
    private final Logger logger = LoggerFactory.getLogger(TimeSeriesStoreOperation.class);

    private final RiakKvPB.TsPutReq.Builder reqBuilder;

    private TimeSeriesStoreOperation(Builder builder)
    {
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
    protected RiakMessage createChannelMessage() {
        RiakKvPB.TsPutReq req = reqBuilder.build();
        return new RiakMessage(RiakMessageCodes.MSG_TsPutReq, req.toByteArray());
    }

    @Override
    protected RiakKvPB.TsPutResp decode(RiakMessage rawMessage) {
        Operations.checkMessageType(rawMessage, RiakMessageCodes.MSG_TsPutResp);

        try
        {
            byte[] data = rawMessage.getData();

            if (data.length == 0) // not found
            {
                return null;
            }

            return RiakKvPB.TsPutResp.parseFrom(data);
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
        private final TimeSeriesConverter converter = new TimeSeriesConverter();

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
            this.reqBuilder.addAllColumns(this.converter.convertColumns(columns));
            return this;
        }

        public Builder withRows(Collection<Row> rows)
        {
            this.reqBuilder.addAllRows(this.converter.convertRows(rows));
            return this;
        }

        public TimeSeriesStoreOperation build()
        {
            return new TimeSeriesStoreOperation(this);
        }
    }
}


