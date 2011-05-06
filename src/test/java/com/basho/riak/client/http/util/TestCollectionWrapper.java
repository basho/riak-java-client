/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.client.http.util;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.basho.riak.client.http.util.CollectionWrapper;
import com.basho.riak.client.util.CharsetUtils;

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
            String el = CharsetUtils.asUTF8String(bytes);
            els.add(el);
            impl.cache(el);
            return true;
        }
        @Override protected void closeBackend() { }
    }
}
