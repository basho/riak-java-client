package com.basho.riak.client.raw;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.junit.Test;

import com.basho.riak.client.response.HttpResponse;

public class TestRawStoreResponse {

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
