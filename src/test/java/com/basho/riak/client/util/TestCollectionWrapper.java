package com.basho.riak.client.util;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

public class TestCollectionWrapper {

    final static int MAX_ELS = 10;
    List<String> els = new ArrayList<String>();
    CollectionWrapper<String> impl;
    
    @Before public void setup() {
        impl = spy(new CollectionWrapperStub<String>());
    }
    
    @Test public void add_adds_to_collection() {
        final String el = "element";
        impl.add(el);
        assertSame(el, impl.iterator().next());
    }
    
    @Test public void iterator_returns_all_elements() {
        Iterator<String> iter = impl.iterator();
        for (int i = 0; i < MAX_ELS; i++) {
            String s = iter.next();
            assertEquals(els.get(i), s);
        }
        assertArrayEquals(els.toArray(), impl.toArray());
    }

    @Test public void to_array_returns_all_elements() {
        Object[] s = impl.toArray();
        assertArrayEquals(els.toArray(), s);
    }

    class CollectionWrapperStub<T> extends CollectionWrapper<T> {
        @Override protected boolean cacheNext() {
            if (els.size() >= MAX_ELS)
                return false;
            byte[] bytes = new byte[10];
            new Random().nextBytes(bytes);
            String el = new String(bytes);
            els.add(el);
            impl.cache(el);
            return true;
        }
        @Override protected void closeBackend() { }
    }
}
