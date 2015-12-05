package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.protobuf.RiakTsPB;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public final class PbResultFactory
{
    private PbResultFactory() {}

    public static QueryResult convertPbQueryResp(RiakTsPB.TsQueryResp response)
    {
        return response == null ? QueryResult.EMPTY : new QueryResult(response.getColumnsList(),
                                                                      response.getRowsList());
    }

    public static QueryResult convertPbGetResp(RiakTsPB.TsGetResp response)
    {
        return response == null ? QueryResult.EMPTY : new QueryResult(response.getColumnsList(),
                                                                      response.getRowsList());
    }

    public static QueryResult convertPbListKeysResp(List<RiakTsPB.TsListKeysResp> responseChunks)
    {
        if (responseChunks == null)
        {
            return QueryResult.EMPTY;
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
