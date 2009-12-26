package com.basho.riak.client.jiak;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.json.JSONException;
import org.junit.Test;

import com.basho.riak.client.response.HttpResponse;

public class TestJiakStoreResponse {

    @Test public void doesnt_throw_on_null_impl() throws JSONException {
        new JiakStoreResponse(null);
    }

    @Test public void parses_meta_fields() throws JSONException {
        final HttpResponse mockHttpResponse = mock(HttpResponse.class);
        final String BODY = 
            "{\"vclock\":\"a85hYGBgzGDKBVIsbHMUezKYEhnzWBm6c1Yc4csCAA==\"," +
            "\"lastmod\":\"Tue, 22 Dec 2009 18:48:37 GMT\"," +
            "\"vtag\":\"611xKewKUJFCta8Haxs42d\"}";

        when(mockHttpResponse.getBody()).thenReturn(BODY);
        when(mockHttpResponse.isSuccess()).thenReturn(true);

        JiakStoreResponse impl = new JiakStoreResponse(mockHttpResponse);

        assertEquals("a85hYGBgzGDKBVIsbHMUezKYEhnzWBm6c1Yc4csCAA==", impl.getVclock());
        assertEquals("Tue, 22 Dec 2009 18:48:37 GMT", impl.getLastmod());
        assertEquals("611xKewKUJFCta8Haxs42d", impl.getVtag());
    }

}
