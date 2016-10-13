package com.basho.riak.client.api;

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
 * @param <Q> The query info type
 * @author Dave Rusek
 * @author Brian Roach <roach at basho.com>
 * @since 2.0
 */
public abstract class StreamableRiakCommand<S, R, Q> extends RiakCommand<R, Q>
{
    protected abstract RiakFuture<S, Q> executeAsyncStreaming(RiakCluster cluster, int timeout);
}
