package com.basho.riak.client.raw;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.basho.riak.client.response.HttpResponse;

public class TestRawFetchResponse {

    final String BUCKET = "bucket";
    final String KEY = "key";
    final Map<String, String> SINGLE_HEADERS = new HashMap<String, String>();
    final String SINGLE_BODY = "foo";
    final Map<String, String> SIBLING_HEADERS = new HashMap<String, String>();
    String SIBLING_BODY;
    @Mock HttpResponse mockHttpResponse;
    
    @Before public void setup() {
        MockitoAnnotations.initMocks(this);
        
        SINGLE_HEADERS.put("X-Riak-Vclock".toLowerCase(), "a85hYGBgzGDKBVIsDPKZOzKYEhnzWBlaJyw9wpcFAA==");
        SINGLE_HEADERS.put("Vary".toLowerCase(), "Accept-Encoding");
        SINGLE_HEADERS.put("Server".toLowerCase(), "MochiWeb/1.1 WebMachine/1.5.1 (hack the charles gibson)");
        SINGLE_HEADERS.put("Link".toLowerCase(), "</raw/b/l>; riaktag=\"next\", </raw/b>; rel=\"up\"");
        SINGLE_HEADERS.put("Last-Modified".toLowerCase(), "Tue, 22 Dec 2009 18:48:37 GMT");
        SINGLE_HEADERS.put("ETag".toLowerCase(), "4d5y9wqQK2Do0RK5ezwCJD");
        SINGLE_HEADERS.put("Date".toLowerCase(), "Tue, 22 Dec 2009 19:06:47 GMT");
        SINGLE_HEADERS.put("Content-Type".toLowerCase(), "text/plain");
        SINGLE_HEADERS.put("Content-Length".toLowerCase(), "3");

        SIBLING_HEADERS.put("X-Riak-Vclock".toLowerCase(), "a85hYGBgzmDKBVIsDPKZOzKYEhnzWBlaJyw9wgcVZtWdug4q/GgGXJitOYmh6u0rZIksAA==");
        SIBLING_HEADERS.put("Vary".toLowerCase(), "Accept, Accept-Encoding");
        SIBLING_HEADERS.put("Server".toLowerCase(), "MochiWeb/1.1 WebMachine/1.5.1 (hack the charles gibson)");
        SIBLING_HEADERS.put("Date".toLowerCase(), "Tue, 22 Dec 2009 19:24:21 GMT");
        SIBLING_HEADERS.put("Content-Type".toLowerCase(), "multipart/mixed; boundary=1MFeoR33D8Jdz3uUa9SQI7H8XCb");
        SIBLING_HEADERS.put("Content-Length".toLowerCase(), "407");
        
        SIBLING_BODY = "\n" +
            "--1MFeoR33D8Jdz3uUa9SQI7H8XCb\n" +
            "Content-Type: text/plain\n" +
            "Link: </raw/b>; rel=\"up\", </raw/b/l>; riaktag=\"next\"\n" +
            "Etag: 55SrI4GjdnGfyuShLBWjuf\n" +
            "Last-Modified: Tue, 22 Dec 2009 19:24:18 GMT\n" +
            "\n" +
            "bar\n" +
            "--1MFeoR33D8Jdz3uUa9SQI7H8XCb\n" +
            "Content-Type: application/octect-stream\n" +
            "Link: </raw/b>; rel=\"up\"\n" +
            "Etag: 4d5y9wqQK2Do0RK5ezwCJD\n" +
            "Last-Modified: Tue, 22 Dec 2009 18:48:37 GMT\n" +
            "X-Riak-Meta-Test: value\n" +
            "\n" +
            "foo\n" +
            "--1MFeoR33D8Jdz3uUa9SQI7H8XCb--\n";
    }

    @Test public void doesnt_throw_on_null_impl() throws JSONException {
        new RawFetchResponse(null);
    }
    
    @Test public void parses_single_object() throws JSONException {
        when(mockHttpResponse.getBucket()).thenReturn(BUCKET);
        when(mockHttpResponse.getKey()).thenReturn(KEY);
        when(mockHttpResponse.getHttpHeaders()).thenReturn(SINGLE_HEADERS);
        when(mockHttpResponse.getBody()).thenReturn(SINGLE_BODY);
        when(mockHttpResponse.isSuccess()).thenReturn(true);
        
        RawFetchResponse impl = new RawFetchResponse(mockHttpResponse);
        
        assertTrue(impl.hasObject());
        assertEquals(BUCKET, impl.getObject().getBucket());
        assertEquals(KEY, impl.getObject().getKey());
        assertEquals("text/plain", impl.getObject().getContentType());
        assertEquals(SINGLE_BODY, impl.getObject().getValue());
        assertEquals("Tue, 22 Dec 2009 18:48:37 GMT", impl.getObject().getLastmod());
        assertEquals(1, impl.getObject().getLinks().size());
        assertEquals(0, impl.getObject().getUsermeta().size());
        assertEquals("a85hYGBgzGDKBVIsDPKZOzKYEhnzWBlaJyw9wpcFAA==", impl.getObject().getVclock());
        assertEquals("4d5y9wqQK2Do0RK5ezwCJD", impl.getObject().getVtag());
    }
    
    @Test public void parses_sibling_objects() throws JSONException {
        when(mockHttpResponse.getBucket()).thenReturn(BUCKET);
        when(mockHttpResponse.getKey()).thenReturn(KEY);
        when(mockHttpResponse.getHttpHeaders()).thenReturn(SIBLING_HEADERS);
        when(mockHttpResponse.getBody()).thenReturn(SIBLING_BODY);
        when(mockHttpResponse.getStatusCode()).thenReturn(300);
        
        RawFetchResponse impl = new RawFetchResponse(mockHttpResponse);
        List<RawObject> siblings = impl.getSiblings();
        
        assertTrue(impl.hasSiblings());

        RawObject o;
        o = siblings.get(0);
        assertEquals(BUCKET, o.getBucket());
        assertEquals(KEY, o.getKey());
        assertEquals("text/plain", o.getContentType());
        assertEquals("bar", o.getValue());
        assertEquals(1, o.getLinks().size());
        assertEquals(0, o.getUsermeta().size());
        assertEquals("Tue, 22 Dec 2009 19:24:18 GMT", o.getLastmod());
        assertEquals("a85hYGBgzmDKBVIsDPKZOzKYEhnzWBlaJyw9wgcVZtWdug4q/GgGXJitOYmh6u0rZIksAA==", o.getVclock());
        assertEquals("55SrI4GjdnGfyuShLBWjuf", o.getVtag());

        o = siblings.get(1);
        assertEquals(BUCKET, o.getBucket());
        assertEquals(KEY, o.getKey());
        assertEquals("application/octect-stream", o.getContentType());
        assertEquals("foo", o.getValue());
        assertEquals("Tue, 22 Dec 2009 18:48:37 GMT", o.getLastmod());
        assertEquals(0, o.getLinks().size());
        assertEquals(1, o.getUsermeta().size());
        assertEquals("value", o.getUsermeta().get("test"));
        assertEquals("a85hYGBgzmDKBVIsDPKZOzKYEhnzWBlaJyw9wgcVZtWdug4q/GgGXJitOYmh6u0rZIksAA==", o.getVclock());
        assertEquals("4d5y9wqQK2Do0RK5ezwCJD", o.getVtag());
    }

    @Test public void chooses_one_object_when_siblings_exist() throws JSONException {
        when(mockHttpResponse.getBucket()).thenReturn(BUCKET);
        when(mockHttpResponse.getKey()).thenReturn(KEY);
        when(mockHttpResponse.getHttpHeaders()).thenReturn(SIBLING_HEADERS);
        when(mockHttpResponse.getBody()).thenReturn(SIBLING_BODY);
        when(mockHttpResponse.getStatusCode()).thenReturn(300);
        
        RawFetchResponse impl = new RawFetchResponse(mockHttpResponse);
        
        assertTrue(impl.hasObject());
        assertTrue(impl.getSiblings().contains(impl.getObject()));
    }
}