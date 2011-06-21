package com.basho.riak.client.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * @param <T>
 *
 * @deprecated with the addition of a protocol buffers client in 0.14 all the
 *             existing REST client code should be in client.http.* this class
 *             has therefore been moved. Please use
 *             com.basho.riak.client.http.util.CollectionWrapper
 *             instead.
 *             <p>WARNING: This class will be REMOVED in the next version.</p>
 * @see com.basho.riak.client.http.util.CollectionWrapper
 */
@Deprecated
public abstract class CollectionWrapper<T> implements Collection<T> {

    List<T> cache = new ArrayList<T>();

    /**
     * Cache one or more objects from the backend by calling cache(T)
     * 
     * @return true if an object was added to the cache; false otherwise.
     */
    abstract protected boolean cacheNext();

    /**
     * Close the backend so no more objects can be read from it (getNext()
     * should only return null afterwards). Called by clear().
     */
    abstract protected void closeBackend();

    /**
     * Called by subclasses to add an object to the cache when executing cacheNext().
     */
    protected void cache(T object) {
        cache.add(object);
    }

    public boolean add(T e) {
        return cache.add(e);
    }

    public boolean addAll(Collection<? extends T> c) {
        return cache.addAll(c);
    }

    public void clear() {
        cache.clear();
        closeBackend();
    }

    public boolean contains(Object o) {
        if (cache.contains(o))
            return true;

        cacheAll();
        return cache.contains(o);
    }

    public boolean containsAll(Collection<?> c) {
        if (cache.containsAll(c))
            return true;

        cacheAll();
        return cache.containsAll(c);
    }

    public boolean isEmpty() {
        cacheAll();
        return cache.isEmpty();
    }

    public Iterator<T> iterator() {
        return new WrappedCollectionIterator();
    }

    public boolean remove(Object o) {
        if (contains(o))
            return cache.remove(o);

        cacheAll();
        return cache.remove(o);
    }

    public boolean removeAll(Collection<?> c) {
        cacheAll();
        return cache.removeAll(c);
    }

    public boolean retainAll(Collection<?> c) {
        cacheAll();
        return cache.retainAll(c);
    }

    public int size() {
        cacheAll();
        return cache.size();
    }

    public Object[] toArray() {
        cacheAll();
        return cache.toArray();
    }

    public <A> A[] toArray(A[] a) {
        cacheAll();
        return cache.toArray(a);
    }

    List<T> getCache() {
        return this.cache;
    }

    /**
     * Reads and caches all the of keys from the input stream
     */
    void cacheAll() {
        while (cacheNext()) { /* nop */}
    }

    class WrappedCollectionIterator implements Iterator<T> {

        int index = 0;
        boolean removed = false;

        public boolean hasNext() {
            if (index < cache.size())
                return true;

            return cacheNext();
        }

        public T next() {
            removed = false;
            while (index >= cache.size() && cacheNext()) { /* nop */}
            if (index < cache.size())
                return cache.get(index++);
            return null;
        }

        public void remove() {
            if (!removed && (index > 0) && (index <= cache.size())) {
                index--;
                cache.remove(index);
                removed = true;
            }
        }

    }
}
