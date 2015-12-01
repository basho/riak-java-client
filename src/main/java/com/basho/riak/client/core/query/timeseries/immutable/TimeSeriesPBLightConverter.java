package com.basho.riak.client.core.query.timeseries.immutable;

import com.basho.riak.client.core.query.timeseries.IQueryResult;
import com.basho.riak.protobuf.RiakTsPB;


/**
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public final class TimeSeriesPBLightConverter
{
    private TimeSeriesPBLightConverter() {}

    public static IQueryResult convertPbGetResp(RiakTsPB.TsQueryResp response)
    {
        return new QueryResultLight(response);
    }
}
