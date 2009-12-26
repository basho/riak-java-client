package com.basho.riak.client.jiak;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.json.JSONException;
import org.junit.Test;

import com.basho.riak.client.response.HttpResponse;

public class TestJiakFetchResponse {

    @Test public void doesnt_throw_on_null_impl() throws JSONException {
        new JiakFetchResponse(null);
    }
    
    @Test public void parses_object() throws JSONException {
        final HttpResponse mockHttpResponse = mock(HttpResponse.class);
        final String BUCKET = "bucket";
        final String KEY = "key";
        final String BODY = 
            "{\"object\":" +
                "{\"value1\":1," +
                "\"usermeta\":" +
                    "{\"test\":\"v\"}}," +
            "\"vclock\":\"a85hYGBgzGDKBVIsbHMUezKYEhnzWBm6c1Yc4csCAA==\"," +
            "\"lastmod\":\"Tue, 22 Dec 2009 18:48:37 GMT\"," +
            "\"vtag\":\"611xKewKUJFCta8Haxs42d\"," +
            "\"bucket\":\"" + BUCKET + "\"," +
            "\"key\":\"" + KEY + "\"," +
            "\"links\":[[\"b\", \"l\", \"tag\"]]}";

        when(mockHttpResponse.getBucket()).thenReturn(BUCKET);
        when(mockHttpResponse.getKey()).thenReturn(KEY);
        when(mockHttpResponse.getBody()).thenReturn(BODY);
        when(mockHttpResponse.isSuccess()).thenReturn(true);

        JiakFetchResponse impl = new JiakFetchResponse(mockHttpResponse);
        
        assertTrue(impl.hasObject());
        assertEquals(BUCKET, impl.getObject().getBucket());
        assertEquals(KEY, impl.getObject().getKey());
        assertEquals(1, impl.getObject().getValueAsJSON().getInt("value1"));
        assertEquals(1, impl.getObject().getLinks().size());
        assertEquals("b", impl.getObject().getLinks().iterator().next().getBucket());
        assertEquals("l", impl.getObject().getLinks().iterator().next().getKey());
        assertEquals("tag", impl.getObject().getLinks().iterator().next().getTag());
        assertEquals(1, impl.getObject().getUsermeta().size());
        assertEquals("v", impl.getObject().getUsermetaAsJSON().getString("test"));
        assertEquals("Tue, 22 Dec 2009 18:48:37 GMT", impl.getObject().getLastmod());
        assertEquals("a85hYGBgzGDKBVIsbHMUezKYEhnzWBm6c1Yc4csCAA==", impl.getObject().getVclock());
        assertEquals("611xKewKUJFCta8Haxs42d", impl.getObject().getVtag());
    }

}
