/*
 * Copyright 2013-2016 Basho Technologies Inc
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

import com.basho.riak.client.api.commands.CoreFutureAdapter;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.PBStreamingFutureOperation;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;


/**
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.1.0
 */
public abstract class GenericRiakCommand<R, I, CoreR, CoreI> extends RiakCommand<R, I>
{
    public static abstract class GenericRiakCommandWithSameInfo<R, I, CoreR> extends GenericRiakCommand<R,I, CoreR, I>
    {
        @Override
        protected I convertInfo(I coreInfo) {
            return coreInfo;
        }
    }

    protected abstract FutureOperation<CoreR, ?, CoreI> buildCoreOperation();

    protected RiakFuture<R,I> executeAsync(RiakCluster cluster)
    {
        final FutureOperation<CoreR, ?, CoreI> coreOperation = buildCoreOperation();
        assert coreOperation != null;

        // TODO: WE NEED TO GET RID SUCH A WEIRD IF-FORK
        final RiakFuture<CoreR, CoreI> coreFuture;
        if (coreOperation instanceof PBStreamingFutureOperation)
        {
            coreFuture = cluster.execute((PBStreamingFutureOperation<CoreR, ?, CoreI>) coreOperation);
        }
        else
        {
            coreFuture = cluster.execute(coreOperation);
        }

        assert coreFuture != null;

        final CoreFutureAdapter<R, I, CoreR, CoreI> future =
                new CoreFutureAdapter<R, I, CoreR, CoreI>(coreFuture)
                {
                    @Override
                    protected R convertResponse(CoreR coreResponse)
                    {
                        return GenericRiakCommand.this.convertResponse(coreResponse);
                    }

                    @Override
                    protected I convertQueryInfo(CoreI coreQueryInfo)
                    {
                        return GenericRiakCommand.this.convertInfo(coreQueryInfo);
                    }
                };
        coreFuture.addListener(future);
        return future;
    }

    protected abstract R convertResponse(CoreR coreResponse);

    protected abstract I convertInfo(CoreI coreInfo);
}