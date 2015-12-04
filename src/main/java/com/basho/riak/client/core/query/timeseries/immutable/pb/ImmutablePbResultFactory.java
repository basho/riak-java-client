package com.basho.riak.client.core.query.timeseries.immutable.pb;

import com.basho.riak.client.core.query.timeseries.IQueryResult;
import com.basho.riak.protobuf.RiakTsPB;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public final class ImmutablePbResultFactory
{
    private ImmutablePbResultFactory() {}

    public static IQueryResult convertPbQueryResp(RiakTsPB.TsQueryResp response)
    {
        return response == null ?
                QueryResult.EmptyResult :
                new QueryResult(response.getColumnsList(), response.getRowsList());
    }

    public static IQueryResult convertPbGetResp(RiakTsPB.TsGetResp response)
    {
        return response == null ?
                QueryResult.EmptyResult :
                new QueryResult(response.getColumnsList(), response.getRowsList());
    }

    public static IQueryResult convertPbListKeysResp(List<RiakTsPB.TsListKeysResp> responseChunks)
    {
        if(responseChunks == null)
        {
            return QueryResult.EmptyResult;
        }

        int size = 0;
        for (RiakTsPB.TsListKeysResp resp : responseChunks)
        {
            size += resp.getKeysCount();
        }

        final ArrayList<RiakTsPB.TsRow> pbRows = new ArrayList<RiakTsPB.TsRow>(size);

        for (RiakTsPB.TsListKeysResp resp : responseChunks)
        {
            pbRows.addAll(resp.getKeysList());
        }

        return new QueryResult(pbRows);
    }
}
