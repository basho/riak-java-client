/*
 * Copyright 2016 Basho Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
