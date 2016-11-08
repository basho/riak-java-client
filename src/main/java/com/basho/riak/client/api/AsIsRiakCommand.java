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

import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.PBStreamingFutureOperation;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.1.0
 */
public abstract class AsIsRiakCommand<R, I> extends RiakCommand<R, I>
{
    protected abstract FutureOperation<R, ?, I> buildCoreOperation();

    protected RiakFuture<R,I> executeAsync(RiakCluster cluster)
    {
        final FutureOperation<R, ?, I> coreOperation = buildCoreOperation();

        // TODO: WE NEED TO GET RID SUCH A WEIRD IF-FORK
        final RiakFuture<R, I> coreFuture;
        if (coreOperation instanceof PBStreamingFutureOperation)
        {
            coreFuture = cluster.execute((PBStreamingFutureOperation<R, ?, I>) coreOperation);
        }
        else
        {
            coreFuture = cluster.execute(coreOperation);
        }

        return coreFuture;
    }

}
