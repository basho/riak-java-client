/*
 * Copyright 2013 Basho Technologies Inc
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

package com.basho.riak.client.api.commands;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.FetchBucketPropsOperation;
import com.basho.riak.client.core.query.Namespace;


 /*
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class FetchBucketProperties extends RiakCommand<FetchBucketPropsOperation.Response, Namespace>
{

	private final Namespace namespace;

	public FetchBucketProperties(Builder builder)
	{
		this.namespace = builder.namespace;
	}

	@Override
    protected final RiakFuture<FetchBucketPropsOperation.Response, Namespace> executeAsync(RiakCluster cluster)
    {
        RiakFuture<FetchBucketPropsOperation.Response, Namespace> coreFuture =
            cluster.execute(buildCoreOperation());
        
        CoreFutureAdapter<FetchBucketPropsOperation.Response, Namespace, FetchBucketPropsOperation.Response, Namespace> future =
            new CoreFutureAdapter<FetchBucketPropsOperation.Response, Namespace, FetchBucketPropsOperation.Response, Namespace>(coreFuture)
            {
                @Override
                protected FetchBucketPropsOperation.Response convertResponse(FetchBucketPropsOperation.Response coreResponse)
                {
                    return coreResponse;
                }

                @Override
                protected Namespace convertQueryInfo(Namespace coreQueryInfo)
                {
                    return coreQueryInfo;
                }
            };
        coreFuture.addListener(future);
        return future;
    }
    
    private FetchBucketPropsOperation buildCoreOperation()
    {
        return new FetchBucketPropsOperation.Builder(namespace).build();
    }

	public static class Builder
	{
		private final Namespace namespace;

		public Builder(Namespace namespace)
		{
			if (namespace == null)
            {
                throw new IllegalArgumentException("Namespace cannot be null");
            }
            this.namespace = namespace;
		}

		public FetchBucketProperties build()
		{
			return new FetchBucketProperties(this);
		}
	}

}
