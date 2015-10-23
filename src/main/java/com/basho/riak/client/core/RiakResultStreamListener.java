package com.basho.riak.client.core;

/**
 * Listener interface for forwarding Streaming operation/command result chunks.
 * @param <T> The type that the stream chunk returns.
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public interface RiakResultStreamListener<T> {
    void handle(T response);
}
