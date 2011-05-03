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
package com.basho.riak.client.http.response;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.json.JSONTokener;
import org.junit.Test;

import com.basho.riak.client.http.response.StreamedKeysCollection;

public class TestStreamedKeysCollection {
    
    StreamedKeysCollection impl;
    
    @Test public void gets_all_keys() {
        final String keys = "{\"keys\":[\"key1\"]}{\"keys\":[]}{\"keys\":[]}{\"keys\":[\"key2\",\"key3\"]}{\"keys\":[]}";
        final InputStream stream = new ByteArrayInputStream(keys.getBytes());
        impl = new StreamedKeysCollection(new JSONTokener(new InputStreamReader(stream)));
        Iterator<String> iter = impl.iterator();

        // cause all keys to be cached
        assertEquals(3, impl.size());
        assertEquals("key1", iter.next());
        assertEquals("key2", iter.next());
        assertEquals("key3", iter.next());
    }

    @Test public void iterator_iterates_all_keys() {
        final String keys = "{\"keys\":[\"key1\"]}{\"keys\":[]}{\"keys\":[]}{\"keys\":[\"key2\",\"key3\"]}{\"keys\":[]}";
        final InputStream stream = new ByteArrayInputStream(keys.getBytes());
        StreamedKeysCollection impl = new StreamedKeysCollection(new JSONTokener(new InputStreamReader(stream)));

        int i = 0;
        String expected[] = {"key1", "key2", "key3"};
        for (String key : impl) {
            assertEquals(expected[i++], key);
        }
    }
    
    @Test public void reads_an_input_array() {
        final String keys = "[\"key1\", \"key2\"]";
        final InputStream stream = new ByteArrayInputStream(keys.getBytes());
        StreamedKeysCollection impl = new StreamedKeysCollection(new JSONTokener(new InputStreamReader(stream)));
        Iterator<String> iter = impl.iterator();
        
        assertEquals("key1", iter.next());
        assertEquals("key2", iter.next());
    }
    
    @Test public void cacheNext_finds_first_embedded_array() {
        final String keys = "{\"keys\":[\"key1\",\"key2\",\"key3\"]}";
        final InputStream stream = new ByteArrayInputStream(keys.getBytes());
        StreamedKeysCollection impl = new StreamedKeysCollection(new JSONTokener(new InputStreamReader(stream)));

        assertEquals("key1", impl.iterator().next());
    }

    @Test public void finds_next_array() {
        final String keys = "{\"keys\":[\"key1\"]}{\"j\": 1, \"k\": \"v\", \"l\": [ ]}[\"key2\", \"key3\"]";
        final InputStream stream = new ByteArrayInputStream(keys.getBytes());
        StreamedKeysCollection impl = new StreamedKeysCollection(new JSONTokener(new InputStreamReader(stream)));
        Iterator<String> iter = impl.iterator();
        
        assertEquals("key1", iter.next());
        assertEquals("key2", iter.next());
        assertEquals("key3", iter.next());
    }

    @Test public void cache_next_returns_false_after_calling_close_backend() {
        final String keys = "[\"key1\", \"key2\"]";
        final InputStream stream = new ByteArrayInputStream(keys.getBytes());

        impl = new StreamedKeysCollection(new JSONTokener(new InputStreamReader(stream)));
        assertTrue(impl.cacheNext());
        
        impl.closeBackend();
        assertFalse(impl.cacheNext());
        
        assertEquals("key1", impl.iterator().next());
    }
}
