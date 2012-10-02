package com.basho.riak.client.query.indexes;

import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.MapMaker;

/**
 * A concurrent map that provides fast access to simple locks base on a keys for the style synchronized(object){...}
 * @author Guido Medina
 * @param <K> The type of key to use which must be Comparable.
 */
@SuppressWarnings("rawtypes") public class LockMap<K extends Comparable> {
    private final ConcurrentMap<K, Object> locks;

    public LockMap() {
        this(16, 64);
    }

    public LockMap(final int concurrencyLevel) {
        this(concurrencyLevel, 64);
    }

    public LockMap(final int concurrencyLevel, final int initialCapacity) {
        locks = new MapMaker().concurrencyLevel(concurrencyLevel).initialCapacity(initialCapacity).weakValues().makeMap();
    }

    public Object getLock(final K key) {
        final Object object = new Object();
        Object lock = locks.putIfAbsent(key, object);
        return lock == null ? object : lock;
    }

}
