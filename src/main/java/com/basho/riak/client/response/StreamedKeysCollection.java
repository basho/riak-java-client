package com.basho.riak.client.response;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.json.JSONException;
import org.json.JSONTokener;

/**
 * Presents the stream of keys from a Riak bucket response with query parameter
 * keys=stream as a collection. Keys are read from the stream as needed. Note,
 * this class is NOT thread-safe!
 */
public class StreamedKeysCollection implements Collection<String> {

    List<String> cache = new ArrayList<String>();
    JSONTokener tokens;
    boolean readingArray = false;

    public StreamedKeysCollection(JSONTokener tokens) {
        this.tokens = tokens;
    }

    public boolean add(String e) {
        return cache.add(e);
    }

    public boolean addAll(Collection<? extends String> c) {
        return cache.addAll(c);
    }

    public void clear() {
        cache.clear();
        tokens = null;
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
        if (tokens == null && cache.isEmpty())
            return true;

        cacheAll();
        return cache.isEmpty();
    }

    public Iterator<String> iterator() {
        return new StreamedKeysIterator();
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

    public <T> T[] toArray(T[] a) {
        cacheAll();
        return cache.toArray(a);
    }

    List<String> getCache() {
        return this.cache;
    }
    
    /**
     * Reads and caches all the of keys from the input stream
     */
    void cacheAll() {
        while (cacheNext()) { /* nop */}
    }
    
    /**
     * Tries to read and cache another set of keys from the input stream. This
     * function is actually just a hacked-up implementation that finds the next
     * available array in the stream and sucks elements out of it.
     * 
     * @return true if keys were added to the cache; false otherwise.
     */
    boolean cacheNext() {
        if (tokens == null)
            return false;

        try {
            while (!tokens.end()) {
                char c = tokens.nextClean();
                if ((!readingArray && c == '[') || (readingArray && c == ',')) {
                    if (tokens.nextClean() != ']') {
                        tokens.back();
                        readingArray = true;
                        cache.add(tokens.nextValue().toString());
                        return true;
                    }
                } else if (readingArray && c == ']') {
                    readingArray = false;
                } else if (c == '\\') {
                    tokens.nextClean(); // skip over escaped chars
                }
            }
        } catch (JSONException e) { /* nop */}
        
        return false;
    }

    /**
     * Implements the iterator interface for this set of streamed keys.
     */
    class StreamedKeysIterator implements Iterator<String> {

        int index = 0;
        boolean removed = false;

        public boolean hasNext() {
            if (index < cache.size())
                return true;

            return cacheNext();
        }

        public String next() {
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
