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
package com.basho.riak.client.response;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.util.Multipart;
import com.basho.riak.client.util.StreamedMultipart;

public class TestStreamedSiblingsCollection {

    @Mock RiakClient mockRiak;
    @Mock StreamedMultipart mockMultipart;
    @Mock Multipart.Part mockPart;
    final String BUCKET = "bucket"; 
    final String KEY = "key"; 
    final Map<String, String> headers = new HashMap<String, String>();
    final Map<String, String> partHeaders = new HashMap<String, String>();

    StreamedSiblingsCollection impl;
    
    @Before public void setup() {
        MockitoAnnotations.initMocks(this);
        when(mockMultipart.getHeaders()).thenReturn(headers);
        when(mockPart.getHeaders()).thenReturn(partHeaders);
        impl = new StreamedSiblingsCollection(mockRiak, BUCKET, KEY, mockMultipart);
    }
    
    @Test public void extracts_vclock_from_parent_document() {
        final String vclock = "a85hYGBgzWDKBVJszUkM2xfyZjAlMuaxMmQ90j/KB5FgScvP18UinJRYhV24CIsw0HTG4McxyBJZAA==";
        headers.put("x-riak-vclock", vclock);
        when(mockMultipart.next()).thenReturn(mockPart);
        RiakObject o = impl.iterator().next();
        assertNotNull(o);
        assertEquals(vclock, o.getVclock());
    }

    @Test public void subpart_location_header_overrides_bucket_and_key() {
        partHeaders.put("location", "/riak/new_bucket/new_key");
        when(mockMultipart.next()).thenReturn(mockPart);
        RiakObject o = impl.iterator().next();
        assertNotNull(o);
        assertEquals("new_bucket", o.getBucket());
        assertEquals("new_key", o.getKey());
    }
    
    @Test public void reads_subpart_content_type() {
        partHeaders.put("content-type", "content type");
        when(mockMultipart.next()).thenReturn(mockPart);
        RiakObject o = impl.iterator().next();
        assertNotNull(o);
        assertEquals("content type", o.getContentType());
    }
    
    @Test public void reads_subpart_last_modified() {
        partHeaders.put("last-modified", "lastmod");
        when(mockMultipart.next()).thenReturn(mockPart);
        RiakObject o = impl.iterator().next();
        assertNotNull(o);
        assertEquals("lastmod", o.getLastmod());
    }

    @Test public void reads_subpart_etag() {
        partHeaders.put("etag", "etag");
        when(mockMultipart.next()).thenReturn(mockPart);
        RiakObject o = impl.iterator().next();
        assertNotNull(o);
        assertEquals("etag", o.getVtag());
    }

    @Test public void reads_subpart_usermeta() {
        partHeaders.put("x-riak-meta-foo", "bar");
        when(mockMultipart.next()).thenReturn(mockPart);
        RiakObject o = impl.iterator().next();
        assertNotNull(o);
        assertEquals("bar", o.getUsermetaItem("foo"));
    }

    @Test public void reads_subpart_links() {
        partHeaders.put("link", "</riak/other_bucket/other_key>; riaktag=foo");
        when(mockMultipart.next()).thenReturn(mockPart);
        RiakObject o = impl.iterator().next();
        assertNotNull(o);
        assertTrue(o.hasLink(new RiakLink("other_bucket", "other_key", "foo")));
    }

    @Test public void reads_subpart_stream() {
        final InputStream mockInputStream = mock(InputStream.class);
        when(mockPart.getStream()).thenReturn(mockInputStream);
        when(mockMultipart.next()).thenReturn(mockPart);
        RiakObject o = impl.iterator().next();
        assertNotNull(o);
        assertSame(mockInputStream, o.getValueStream());
    }
    
    @Test public void returns_empty_collection_when_no_parts() {
        assertEquals(0, impl.size());
        assertFalse(impl.iterator().hasNext());
        assertNull(impl.iterator().next());
    }

    @Test(expected=RiakIORuntimeException.class) public void throws_riak_io_runtime_exception_on_communication_error() {
        IOException e = new IOException();
        when(mockMultipart.next()).thenThrow(new RuntimeException(e));
        impl.iterator().next();
    }

}
