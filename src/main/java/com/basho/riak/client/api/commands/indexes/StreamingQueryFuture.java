package com.basho.riak.client.api.commands.indexes;

import com.basho.riak.client.api.commands.ImmediateCoreFutureAdapter;
import com.basho.riak.client.core.StreamingRiakFuture;
import com.basho.riak.client.core.operations.SecondaryIndexQueryOperation;

public class StreamingQueryFuture<T,S>
        extends ImmediateCoreFutureAdapter<T,S,SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query>
{
    private S indexQuery;

    public StreamingQueryFuture(StreamingRiakFuture<SecondaryIndexQueryOperation.Response,
                         SecondaryIndexQueryOperation.Query> coreFuture,
                         T immediateResponse,
                         S indexQuery)
    {
        super(coreFuture, immediateResponse);
        this.indexQuery = indexQuery;
    }

    @Override
    protected S convertQueryInfo(SecondaryIndexQueryOperation.Query coreQueryInfo)
    {
        return indexQuery;
    }
}
