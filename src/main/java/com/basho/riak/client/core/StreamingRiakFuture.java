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
package com.basho.riak.client.core;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The result of an asynchronous streaming Riak operation.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @param <V> the (response) return type
 * @param <T> The query info type 
 * @since 2.0.5
 */
public interface StreamingRiakFuture<V, T> extends RiakFuture<Void, T>
{
    /**
     * An iterator that provides the stream of results as they return from Riak.
     * @return An iterator.
     */
    Iterator<V> getStreamingResultsIterator();
}
