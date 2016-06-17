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

package com.basho.riak.client.api;

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import java.util.concurrent.ExecutionException;

/**
 * The base class for all Riak Commands.
 * <p>
 * All the commands the {@link RiakClient} can execute extend this class.
 * <h2>Client Commands</h2>
 * <h4>Fetching, storing and deleting objects</h4>
 * <ul>
 * <li>{@link com.basho.riak.client.api.commands.kv.FetchValue}</li>
 * <li>{@link com.basho.riak.client.api.commands.kv.MultiFetch}</li>
 * <li>{@link com.basho.riak.client.api.commands.kv.StoreValue}</li>
 * <li>{@link com.basho.riak.client.api.commands.kv.UpdateValue}</li>
 * <li>{@link com.basho.riak.client.api.commands.kv.DeleteValue}</li>
 * </ul>
 * <h4>Listing keys in a namespace</h4>
 * <ul><li>{@link com.basho.riak.client.api.commands.kv.ListKeys}</li></ul>
 * <h4>Secondary index (2i) commands</h4>
 * <ul>
 * <li>{@link com.basho.riak.client.api.commands.indexes.RawIndexQuery}</li>
 * <li>{@link com.basho.riak.client.api.commands.indexes.BinIndexQuery}</li>
 * <li>{@link com.basho.riak.client.api.commands.indexes.IntIndexQuery}</li>
 * <li>{@link com.basho.riak.client.api.commands.indexes.BigIntIndexQuery}</li>
 * </ul>
 * <h4>Fetching and storing datatypes (CRDTs)</h4>
 * <ul>
 * <li>{@link com.basho.riak.client.api.commands.datatypes.FetchCounter}</li>
 * <li>{@link com.basho.riak.client.api.commands.datatypes.FetchSet}</li>
 * <li>{@link com.basho.riak.client.api.commands.datatypes.FetchMap}</li>
 * <li>{@link com.basho.riak.client.api.commands.datatypes.UpdateCounter}</li>
 * <li>{@link com.basho.riak.client.api.commands.datatypes.UpdateSet}</li>
 * <li>{@link com.basho.riak.client.api.commands.datatypes.UpdateMap}</li>
 * </ul>
 * <h4>Querying and modifying buckets</h4>
 * <ul>
 * <li>{@link com.basho.riak.client.api.commands.buckets.FetchBucketProperties}</li>
 * <li>{@link com.basho.riak.client.api.commands.buckets.StoreBucketProperties}</li>
 * <li>{@link com.basho.riak.client.api.commands.buckets.ListBuckets}</li>
 * </ul>
 * <h4>Search commands</h4>
 * <ul>
 * <li>{@link com.basho.riak.client.api.commands.search.Search}</li>
 * <li>{@link com.basho.riak.client.api.commands.search.FetchIndex}</li>
 * <li>{@link com.basho.riak.client.api.commands.search.StoreIndex}</li>
 * <li>{@link com.basho.riak.client.api.commands.search.DeleteIndex}</li>
 * <li>{@link com.basho.riak.client.api.commands.search.FetchSchema}</li>
 * <li>{@link com.basho.riak.client.api.commands.search.StoreSchema}</li>
* </ul>
* <h4>Map-Reduce</h4>
 * <ul>
 * <li>{@link com.basho.riak.client.api.commands.mapreduce.BucketMapReduce}</li>
 * <li>{@link com.basho.riak.client.api.commands.mapreduce.BucketKeyMapReduce}</li>
 * <li>{@link com.basho.riak.client.api.commands.mapreduce.IndexMapReduce}</li>
 * <li>{@link com.basho.riak.client.api.commands.mapreduce.SearchMapReduce}</li>
 * </ul>
 * </p>
 * @author Dave Rusek
 * @author Brian Roach <roach at basho.com>
 * @param <T> The response type
 * @param <S> The query info type
 * @since 2.0
 */
public abstract class RiakCommand<T,S>
{
    protected final T execute(RiakCluster cluster) throws ExecutionException, InterruptedException
    {
        RiakFuture<T,S> future = executeAsync(cluster);
        future.await();
        return future.get();
    }

    protected abstract RiakFuture<T,S> executeAsync(RiakCluster cluster);
}

