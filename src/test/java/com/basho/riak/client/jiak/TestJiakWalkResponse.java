package com.basho.riak.client.jiak;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.json.JSONException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.basho.riak.client.response.HttpResponse;

public class TestJiakWalkResponse {

    @Mock HttpResponse mockHttpResponse;

    @Before public void setup() {
        MockitoAnnotations.initMocks(this);
    }
    
    @Test public void doesnt_throw_on_null_impl() throws JSONException {
        new JiakWalkResponse(null);
    }

    @Test public void returns_empty_list_on_no_content() throws JSONException {
        when(mockHttpResponse.getBody()).thenReturn(null);
        when(mockHttpResponse.isSuccess()).thenReturn(true);
        JiakWalkResponse impl = new JiakWalkResponse(mockHttpResponse);
        assertEquals(0, impl.getSteps().size());

        when(mockHttpResponse.getBody()).thenReturn("{\"results\": []}");
        when(mockHttpResponse.isSuccess()).thenReturn(true);
        impl = new JiakWalkResponse(mockHttpResponse);
        assertEquals(0, impl.getSteps().size());
    }
    
    @Test public void parses_walk_steps() throws JSONException {
        final String BODY = 
            "{\"results\":" +
                "[" +
                    "[" +
                        "{\"object\":" +
                            "{\"v\":" +
                                "\"foo\"}," +
                            "\"vclock\":\"a85hYGBgzGDKBVIsDJxXZ2YwJTLmsTLcKlhxhC8LAA==\"," +
                            "\"lastmod\":\"Thu, 24 Dec 2009 23:10:18 GMT\"," +
                            "\"vtag\":\"3Uym71iT5d5sV7ZwTe6FQ5\"," +
                            "\"bucket\":\"b\"," +
                            "\"key\":\"k1\"," +
                            "\"links\":[]}," +
                        "{\"object\":" +
                            "{\"v\":" +
                                "\"bar\"}," +
                            "\"vclock\":\"a85hYGBgzGDKBVIs7JYOhzOYEhnzWBluFaw4wpcFAA==\"," +
                            "\"lastmod\":\"Thu, 24 Dec 2009 23:10:18 GMT\"," +
                            "\"vtag\":\"3Yi4k1b4ia0vBKVQlRsGjz\"," +
                            "\"bucket\":\"b\"," +
                            "\"key\":\"k2\"," +
                            "\"links\":[]}" +
                    "]]}";
        
        when(mockHttpResponse.getBody()).thenReturn(BODY);
        when(mockHttpResponse.isSuccess()).thenReturn(true);

        JiakWalkResponse impl = new JiakWalkResponse(mockHttpResponse);
        assertTrue(impl.hasSteps());
        assertEquals(1, impl.getSteps().size());

        assertEquals("b", impl.getSteps().get(0).get(0).getBucket());
        assertEquals("k1", impl.getSteps().get(0).get(0).getKey());
        assertEquals("foo", impl.getSteps().get(0).get(0).getValueAsJSON().getString("v"));

        assertEquals("b", impl.getSteps().get(0).get(1).getBucket());
        assertEquals("k2", impl.getSteps().get(0).get(1).getKey());
        assertEquals("bar", impl.getSteps().get(0).get(1).getValueAsJSON().getString("v"));
    }

    @Test(expected=JSONException.class)
    public void throws_on_malformed_walk_results() throws JSONException {
        when(mockHttpResponse.getBody()).thenReturn("{\"results\": 123}");
        when(mockHttpResponse.isSuccess()).thenReturn(true);

        new JiakWalkResponse(mockHttpResponse);
    }
}
