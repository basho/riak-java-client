package com.basho.riak.client.raw;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;
import org.json.JSONException;
import org.junit.Test;

import com.basho.riak.client.response.HttpResponse;

public class TestRawStoreResponse {

    @Test public void delegates_http_response_methods_to_impl() throws JSONException {
        final String BUCKET = "bucket";
        final String KEY = "key";
        final int STATUS_CODE = 1;
        final Map<String, String> HTTP_HEADERS = new HashMap<String, String>();
        final HttpMethod HTTP_METHOD = mock(HttpMethod.class);
        final boolean IS_SUCCESS = true;
        final boolean IS_ERROR = true;
        
        HttpResponse mockHttpResponse = mock(HttpResponse.class);
        when(mockHttpResponse.getBucket()).thenReturn(BUCKET);
        when(mockHttpResponse.getKey()).thenReturn(KEY);
        when(mockHttpResponse.getBody()).thenReturn(null);
        when(mockHttpResponse.getStatusCode()).thenReturn(STATUS_CODE);
        when(mockHttpResponse.getHttpHeaders()).thenReturn(HTTP_HEADERS);
        when(mockHttpResponse.getHttpMethod()).thenReturn(HTTP_METHOD);
        when(mockHttpResponse.isSuccess()).thenReturn(IS_SUCCESS);
        when(mockHttpResponse.isError()).thenReturn(IS_ERROR);
        
        RawStoreResponse impl = new RawStoreResponse(mockHttpResponse);
        
        assertEquals(BUCKET, impl.getBucket());
        assertEquals(KEY, impl.getKey());
        assertEquals(null, impl.getBody());
        assertEquals(STATUS_CODE, impl.getStatusCode());
        assertSame(HTTP_HEADERS, impl.getHttpHeaders());
        assertSame(HTTP_METHOD, impl.getHttpMethod());
        assertEquals(IS_SUCCESS, impl.isSuccess());
        assertEquals(IS_ERROR, impl.isError());
        
        verify(mockHttpResponse, atLeastOnce()).getBucket();
        verify(mockHttpResponse, atLeastOnce()).getKey();
        verify(mockHttpResponse, atLeastOnce()).getBody();
        verify(mockHttpResponse, atLeastOnce()).getStatusCode();
        verify(mockHttpResponse, atLeastOnce()).getHttpHeaders();
        verify(mockHttpResponse, atLeastOnce()).getHttpMethod();
        verify(mockHttpResponse, atLeastOnce()).isSuccess();
        verify(mockHttpResponse, atLeastOnce()).isError();
    }

    @Test public void doesnt_throw_on_null_impl() throws JSONException {
        new RawFetchResponse(null);
    }
    
    @Test public void parses_meta_headers() {
        final HttpResponse mockHttpResponse = mock(HttpResponse.class);
        final Map<String, String> HTTP_HEADERS = new HashMap<String, String>();
        HTTP_HEADERS.put("X-Riak-Vclock".toLowerCase(), "a85hYGBgzGDKBVIsDPKZOzKYEhnzWBlaJyw9wpcFAA==");
        HTTP_HEADERS.put("Last-Modified".toLowerCase(), "Tue, 22 Dec 2009 18:48:37 GMT");
        HTTP_HEADERS.put("ETag".toLowerCase(), "4d5y9wqQK2Do0RK5ezwCJD");

        when(mockHttpResponse.getHttpHeaders()).thenReturn(HTTP_HEADERS);
        when(mockHttpResponse.isSuccess()).thenReturn(true);

        RawStoreResponse impl = new RawStoreResponse(mockHttpResponse);

        assertEquals("a85hYGBgzGDKBVIsDPKZOzKYEhnzWBlaJyw9wpcFAA==", impl.getVclock());
        assertEquals("Tue, 22 Dec 2009 18:48:37 GMT", impl.getLastmod());
        assertEquals("4d5y9wqQK2Do0RK5ezwCJD", impl.getVtag());
    }
}
