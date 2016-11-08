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

import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;

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

    protected abstract RiakFuture<R, I> executeAsyncStreaming(RiakCluster cluster, int timeout);

    @Override
    protected final FutureOperation<CoreR, ?, CoreI> buildCoreOperation() {
        return buildCoreOperation(false);
    }

    protected abstract  FutureOperation<CoreR, ?, CoreI> buildCoreOperation(boolean streamResults);
}
