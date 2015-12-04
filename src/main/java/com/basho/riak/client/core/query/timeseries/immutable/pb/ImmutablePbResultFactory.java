package com.basho.riak.client.core.query.timeseries.immutable.pb;

import com.basho.riak.client.core.query.timeseries.IQueryResult;
import com.basho.riak.protobuf.RiakTsPB;

import java.util.List;


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

    public static IQueryResult convertPbGetResp(RiakTsPB.TsGetResp response)
    {
        return new QueryResult(response);
    }

    public static IQueryResult convertPbListKeysResp(List<RiakTsPB.TsListKeysResp> responseChunks)
    {
        return new QueryResult(responseChunks);
    }
}
