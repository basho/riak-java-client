package com.basho.riak.client.api.commands.indexes;

import com.basho.riak.client.api.commands.ImmediateCoreFutureAdapter;
import com.basho.riak.client.core.StreamingRiakFuture;
import com.basho.riak.client.core.operations.SecondaryIndexQueryOperation;

/**
 * Streamlined ImmediateCoreFutureAdapter for converting streaming 2i operation results to command results.
 * @param <T> The converted response type.
 * @param <S> The converted query info type.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.1.0
 */
public class Streaming2iQueryFuture<T,S>
        extends ImmediateCoreFutureAdapter<T,S,SecondaryIndexQueryOperation.Response, SecondaryIndexQueryOperation.Query>
{
    private S indexQuery;

    public Streaming2iQueryFuture(StreamingRiakFuture<SecondaryIndexQueryOperation.Response,
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
