package com.basho.riak.client.core.query.timeseries.immutable.pb;

import com.basho.riak.client.core.query.timeseries.IQueryResult;
import com.basho.riak.protobuf.RiakTsPB;


/**
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public final class ImmutablePbResultFactory
{
    private ImmutablePbResultFactory() {}

    public static IQueryResult convertPbGetResp(RiakTsPB.TsQueryResp response)
    {
        return new QueryResult(response);
    }
}
