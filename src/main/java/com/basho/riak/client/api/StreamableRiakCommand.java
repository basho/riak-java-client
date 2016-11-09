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

package com.basho.riak.client.api;

import com.basho.riak.client.api.commands.ImmediateCoreFutureAdapter;
import com.basho.riak.client.core.*;

/*
 * The base class for all Streamable Riak Commands.
 * Allows the user to either use {@link RiakCommand#executeAsync} and return a "batch-mode" result
 * that is only available after the command is complete, or
 * use {@link StreamableRiakCommand#executeAsyncStreaming} and return a "immediate" or "stream-mode" result
 * that data will flow into.
 * @param <S> The response type returned by "streaming mode" {@link executeAsyncStreaming}
 * @param <R> The response type returned by the "batch mode" @{link executeAsync}
 * @param <I> The query info type
 * @author Dave Rusek
 * @author Brian Roach <roach at basho.com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0
 */
public abstract class StreamableRiakCommand<R, I, CoreR, CoreI> extends GenericRiakCommand<R, I, CoreR, CoreI>
{
    public static abstract class StreamableRiakCommandWithSameInfo<R, I, CoreR> extends StreamableRiakCommand<R,I, CoreR, I>
    {
        @Override
        protected I convertInfo(I coreInfo) {
            return coreInfo;
        }
    }

    protected abstract R createResponse(int timeout, StreamingRiakFuture<CoreR, CoreI> coreFuture);

    protected abstract  PBStreamingFutureOperation<CoreR, ?, CoreI> buildCoreOperation(boolean streamResults);

    @Override
    protected final FutureOperation<CoreR, ?, CoreI> buildCoreOperation() {
        return buildCoreOperation(false);
    }

    protected final RiakFuture<R, I> executeAsyncStreaming(RiakCluster cluster, int timeout)
    {
        final PBStreamingFutureOperation<CoreR, ?, CoreI> coreOperation = buildCoreOperation(true);
        final StreamingRiakFuture<CoreR, CoreI> coreFuture = cluster.execute(coreOperation);

        final R r = createResponse(timeout, coreFuture);

        final ImmediateCoreFutureAdapter<R,I, CoreR, CoreI> future = new ImmediateCoreFutureAdapter<R,I,CoreR,CoreI>(coreFuture, r)
        {
            @Override
            protected R convertResponse(CoreR response)
            {
                return StreamableRiakCommand.this.convertResponse(coreOperation, response);
            }

            @Override
            protected I convertQueryInfo(CoreI coreQueryInfo)
            {
                return StreamableRiakCommand.this.convertInfo(coreQueryInfo);
            }
        };

        coreFuture.addListener(future);
        return future;
    }
}
