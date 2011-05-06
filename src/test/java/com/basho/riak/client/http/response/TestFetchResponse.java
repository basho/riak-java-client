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

import  static com.basho.riak.client.util.CharsetUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;
import org.json.JSONException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.basho.riak.client.http.RiakClient;
import com.basho.riak.client.http.RiakObject;
import com.basho.riak.client.http.response.FetchResponse;
import com.basho.riak.client.http.response.HttpResponse;
import com.basho.riak.client.http.response.StreamedSiblingsCollection;
import com.basho.riak.client.http.util.ClientUtils;
import com.basho.riak.client.http.util.Constants;

public class TestFetchResponse {

    final String BUCKET = "bucket";
    final String KEY = "key";
    final Map<String, String> SINGLE_HEADERS = new HashMap<String, String>();
    final String SINGLE_BODY = "foo";
    final Map<String, String> SIBLING_HEADERS = new HashMap<String, String>();
    
    final String CONTENT_LENGTH = "3";
    
    @Mock HttpResponse mockHttpResponse;
    @Mock HttpMethod mockHttpMethod;
    @Mock RiakClient mockRiakClient;
    String SIBLING_BODY;
    
    @Before public void setup() {
        MockitoAnnotations.initMocks(this);
        
        SINGLE_HEADERS.put("X-Riak-Vclock".toLowerCase(), "a85hYGBgzGDKBVIsDPKZOzKYEhnzWBlaJyw9wpcFAA==");
        SINGLE_HEADERS.put("Vary".toLowerCase(), "Accept-Encoding");
        SINGLE_HEADERS.put("Server".toLowerCase(), "MochiWeb/1.1 WebMachine/1.5.1 (hack the charles gibson)");
        SINGLE_HEADERS.put("Link".toLowerCase(), "</riak/b/l>; riaktag=\"next\", </riak/b>; rel=\"up\"");
        SINGLE_HEADERS.put("Last-Modified".toLowerCase(), "Tue, 22 Dec 2009 18:48:37 GMT");
        SINGLE_HEADERS.put("ETag".toLowerCase(), "4d5y9wqQK2Do0RK5ezwCJD");
        SINGLE_HEADERS.put("Date".toLowerCase(), "Tue, 22 Dec 2009 19:06:47 GMT");
        SINGLE_HEADERS.put("Content-Type".toLowerCase(), "text/plain");
        SINGLE_HEADERS.put("Content-Length".toLowerCase(), CONTENT_LENGTH);

        SIBLING_HEADERS.put("X-Riak-Vclock".toLowerCase(), "a85hYGBgzmDKBVIsDPKZOzKYEhnzWBlaJyw9wgcVZtWdug4q/GgGXJitOYmh6u0rZIksAA==");
        SIBLING_HEADERS.put("Vary".toLowerCase(), "Accept, Accept-Encoding");
        SIBLING_HEADERS.put("Server".toLowerCase(), "MochiWeb/1.1 WebMachine/1.5.1 (hack the charles gibson)");
        SIBLING_HEADERS.put("Date".toLowerCase(), "Tue, 22 Dec 2009 19:24:21 GMT");
        SIBLING_HEADERS.put("Content-Type".toLowerCase(), "multipart/mixed; boundary=1MFeoR33D8Jdz3uUa9SQI7H8XCb");
        SIBLING_HEADERS.put("Content-Length".toLowerCase(), "407");
        
        SIBLING_BODY = "\r\n" +
            "--1MFeoR33D8Jdz3uUa9SQI7H8XCb\r\n" +
            "Content-Type: text/plain\r\n" +
            "Link: </riak/b>; rel=\"up\", </riak/b/l>; riaktag=\"next\"\r\n" +
            "Etag: 55SrI4GjdnGfyuShLBWjuf\r\n" +
            "Last-Modified: Tue, 22 Dec 2009 19:24:18 GMT\r\n" +
            "\r\n" +
            "bar\r\n" +
            "--1MFeoR33D8Jdz3uUa9SQI7H8XCb\r\n" +
            "Content-Type: application/octect-stream\r\n" +
            "Link: </riak/b>; rel=\"up\"\r\n" +
            "Etag: 4d5y9wqQK2Do0RK5ezwCJD\r\n" +
            "Last-Modified: Tue, 22 Dec 2009 18:48:37 GMT\r\n" +
            "X-Riak-Meta-Test: value\r\n" +
            "\r\n" +
            "foo\r\n" +
            "--1MFeoR33D8Jdz3uUa9SQI7H8XCb--\r\n";
    }

    @Test public void doesnt_throw_on_null_impl() throws JSONException {
        new FetchResponse(null, null);
    }
    
    @Test public void parses_single_object() throws JSONException {
        when(mockHttpResponse.getBucket()).thenReturn(BUCKET);
        when(mockHttpResponse.getKey()).thenReturn(KEY);
        when(mockHttpResponse.getHttpHeaders()).thenReturn(SINGLE_HEADERS);
        when(mockHttpResponse.getBody()).thenReturn(utf8StringToBytes(SINGLE_BODY));
        when(mockHttpResponse.isSuccess()).thenReturn(true);
        
        FetchResponse impl = new FetchResponse(mockHttpResponse, mockRiakClient);
        
        assertTrue(impl.hasObject());
        assertSame(mockRiakClient, impl.getObject().getRiakClient());
        assertEquals(BUCKET, impl.getObject().getBucket());
        assertEquals(KEY, impl.getObject().getKey());
        assertEquals("text/plain", impl.getObject().getContentType());
        assertEquals(SINGLE_BODY, impl.getObject().getValue());
        assertEquals("Tue, 22 Dec 2009 18:48:37 GMT", impl.getObject().getLastmod());
        assertEquals(1, impl.getObject().numLinks());
        assertFalse(impl.getObject().hasUsermeta());
        assertEquals("a85hYGBgzGDKBVIsDPKZOzKYEhnzWBlaJyw9wpcFAA==", impl.getObject().getVclock());
        assertEquals("4d5y9wqQK2Do0RK5ezwCJD", impl.getObject().getVtag());
    }
    
    @Test public void parses_sibling_objects() throws JSONException {
        when(mockHttpResponse.getBucket()).thenReturn(BUCKET);
        when(mockHttpResponse.getKey()).thenReturn(KEY);
        when(mockHttpResponse.getHttpHeaders()).thenReturn(SIBLING_HEADERS);
        when(mockHttpResponse.getBody()).thenReturn(utf8StringToBytes(SIBLING_BODY));
        when(mockHttpResponse.getBodyAsString()).thenReturn(SIBLING_BODY);
        when(mockHttpResponse.getStatusCode()).thenReturn(300);
        
        FetchResponse impl = new FetchResponse(mockHttpResponse, mockRiakClient);
        assertTrue(impl.hasSiblings());

        Iterator<RiakObject> siblings = impl.getSiblings().iterator();

        RiakObject o;
        o = siblings.next();
        assertSame(mockRiakClient, o.getRiakClient());
        assertEquals(BUCKET, o.getBucket());
        assertEquals(KEY, o.getKey());
        assertEquals("text/plain", o.getContentType());
        assertEquals("bar", o.getValue());
        assertEquals(1, o.numLinks());
        assertFalse(o.hasUsermeta());
        assertEquals("Tue, 22 Dec 2009 19:24:18 GMT", o.getLastmod());
        assertEquals("a85hYGBgzmDKBVIsDPKZOzKYEhnzWBlaJyw9wgcVZtWdug4q/GgGXJitOYmh6u0rZIksAA==", o.getVclock());
        assertEquals("55SrI4GjdnGfyuShLBWjuf", o.getVtag());

        o = siblings.next();
        assertSame(mockRiakClient, o.getRiakClient());
        assertEquals(BUCKET, o.getBucket());
        assertEquals(KEY, o.getKey());
        assertEquals("application/octect-stream", o.getContentType());
        assertEquals("foo", o.getValue());
        assertEquals("Tue, 22 Dec 2009 18:48:37 GMT", o.getLastmod());
        assertFalse(o.hasLinks());
        assertEquals(1, o.numUsermetaItems());
        assertEquals("value", o.getUsermetaItem("test"));
        assertEquals("a85hYGBgzmDKBVIsDPKZOzKYEhnzWBlaJyw9wgcVZtWdug4q/GgGXJitOYmh6u0rZIksAA==", o.getVclock());
        assertEquals("4d5y9wqQK2Do0RK5ezwCJD", o.getVtag());
    }

    @Test public void chooses_one_object_when_siblings_exist() throws JSONException {
        when(mockHttpResponse.getBucket()).thenReturn(BUCKET);
        when(mockHttpResponse.getKey()).thenReturn(KEY);
        when(mockHttpResponse.getHttpHeaders()).thenReturn(SIBLING_HEADERS);
        when(mockHttpResponse.getBody()).thenReturn(utf8StringToBytes(SIBLING_BODY));
        when(mockHttpResponse.getBodyAsString()).thenReturn(SIBLING_BODY);
        when(mockHttpResponse.getStatusCode()).thenReturn(300);
        
        FetchResponse impl = new FetchResponse(mockHttpResponse, mockRiakClient);
        
        assertTrue(impl.hasObject());
        assertTrue(impl.getSiblings().contains(impl.getObject()));
    }
    
    @Test public void stores_value_stream_on_2xx_if_httpresponse_body_is_null() throws IOException {
        final Long contentLength = new Long(100);
        final InputStream mockInputStream = mock(InputStream.class);
        
        SINGLE_HEADERS.put(Constants.HDR_CONTENT_LENGTH, contentLength.toString());
        
        when(mockHttpResponse.isSuccess()).thenReturn(true);
        when(mockHttpResponse.getHttpHeaders()).thenReturn(SINGLE_HEADERS);
        when(mockHttpResponse.isStreamed()).thenReturn(true);
        when(mockHttpResponse.getStream()).thenReturn(mockInputStream);
        
        FetchResponse impl = new FetchResponse(mockHttpResponse, mockRiakClient);
        
        assertTrue(impl.hasObject());
        assertSame(mockInputStream, impl.getObject().getValueStream());
        assertEquals(contentLength, impl.getObject().getValueStreamLength());
    }

    @Test public void returns_streamed_collection_on_streaming_300_response() throws IOException {
        final ByteArrayInputStream is = new ByteArrayInputStream(utf8StringToBytes(SIBLING_BODY));

        when(mockHttpResponse.getStatusCode()).thenReturn(300);
        when(mockHttpResponse.getBucket()).thenReturn(BUCKET);
        when(mockHttpResponse.getKey()).thenReturn(KEY);
        when(mockHttpResponse.getHttpHeaders()).thenReturn(SIBLING_HEADERS);
        when(mockHttpResponse.isStreamed()).thenReturn(true);
        when(mockHttpResponse.getStream()).thenReturn(is);
        
        FetchResponse impl = new FetchResponse(mockHttpResponse, mockRiakClient);
        assertTrue(impl.hasSiblings());
        assertTrue(impl.getSiblings() instanceof StreamedSiblingsCollection);

        Iterator<RiakObject> siblings = impl.getSiblings().iterator();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
 
        RiakObject o;
        o = siblings.next();
        ClientUtils.copyStream(o.getValueStream(), os);
        
        assertSame(mockRiakClient, o.getRiakClient());
        assertEquals(BUCKET, o.getBucket());
        assertEquals(KEY, o.getKey());
        assertEquals("text/plain", o.getContentType());
        assertEquals("bar", os.toString());
        assertEquals(1, o.numLinks());
        assertFalse(o.hasUsermeta());
        assertEquals("Tue, 22 Dec 2009 19:24:18 GMT", o.getLastmod());
        assertEquals("a85hYGBgzmDKBVIsDPKZOzKYEhnzWBlaJyw9wgcVZtWdug4q/GgGXJitOYmh6u0rZIksAA==", o.getVclock());
        assertEquals("55SrI4GjdnGfyuShLBWjuf", o.getVtag());

        o = siblings.next();
        os.reset();
        ClientUtils.copyStream(o.getValueStream(), os);
        assertSame(mockRiakClient, o.getRiakClient());
        assertEquals(BUCKET, o.getBucket());
        assertEquals(KEY, o.getKey());
        assertEquals("application/octect-stream", o.getContentType());
        assertEquals("foo", os.toString());
        assertEquals("Tue, 22 Dec 2009 18:48:37 GMT", o.getLastmod());
        assertFalse(o.hasLinks());
        assertEquals(1, o.numUsermetaItems());
        assertEquals("value", o.getUsermetaItem("test"));
        assertEquals("a85hYGBgzmDKBVIsDPKZOzKYEhnzWBlaJyw9wgcVZtWdug4q/GgGXJitOYmh6u0rZIksAA==", o.getVclock());
        assertEquals("4d5y9wqQK2Do0RK5ezwCJD", o.getVtag());
    }

    @Test public void does_not_close_stream_on_streaming_300_response() throws IOException {
        final ByteArrayInputStream is = new ByteArrayInputStream(utf8StringToBytes(SIBLING_BODY));

        when(mockHttpResponse.getStatusCode()).thenReturn(300);
        when(mockHttpResponse.getBucket()).thenReturn(BUCKET);
        when(mockHttpResponse.getKey()).thenReturn(KEY);
        when(mockHttpResponse.getHttpHeaders()).thenReturn(SIBLING_HEADERS);
        when(mockHttpResponse.isStreamed()).thenReturn(true);
        when(mockHttpResponse.getStream()).thenReturn(is);
        
        new FetchResponse(mockHttpResponse, mockRiakClient);

        verify(mockHttpResponse, never()).close();
   }
    
    @Test public void returns_content_length_for_head() {
        when(mockHttpResponse.getBucket()).thenReturn(BUCKET);
        when(mockHttpResponse.getKey()).thenReturn(KEY);
        when(mockHttpResponse.getHttpHeaders()).thenReturn(SINGLE_HEADERS);
        when(mockHttpResponse.isSuccess()).thenReturn(true);
        
        FetchResponse impl = new FetchResponse(mockHttpResponse, mockRiakClient);
        
        assertSame(mockRiakClient, impl.getObject().getRiakClient());
        assertEquals(BUCKET, impl.getObject().getBucket());
        assertEquals(KEY, impl.getObject().getKey());
        assertEquals("text/plain", impl.getObject().getContentType());
        
        Long contentLength = impl.getObject().getValueStreamLength();
        assertNotNull("Content-Length should not be null", contentLength);
        assertEquals(CONTENT_LENGTH, contentLength.toString());
        assertEquals("Tue, 22 Dec 2009 18:48:37 GMT", impl.getObject().getLastmod());
        assertEquals(1, impl.getObject().numLinks());
        assertFalse(impl.getObject().hasUsermeta());
        assertEquals("a85hYGBgzGDKBVIsDPKZOzKYEhnzWBlaJyw9wpcFAA==", impl.getObject().getVclock());
        assertEquals("4d5y9wqQK2Do0RK5ezwCJD", impl.getObject().getVtag());
    }
}