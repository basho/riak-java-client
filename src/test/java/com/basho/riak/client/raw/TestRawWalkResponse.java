package com.basho.riak.client.raw;

import org.json.JSONException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.basho.riak.client.response.HttpResponse;

public class TestRawWalkResponse {

    @Mock HttpResponse mockHttpResponse;

    @Before public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test public void doesnt_throw_on_null_impl() throws JSONException {
        new RawWalkResponse(null);
    }
    
    @Test public void returns_empty_list_on_no_content() {
        
    }
    
    @Test public void parses_walk_steps() {
        
    }
    
    @Test public void throws_on_invalid_subpart_content_type() {
        
    }
    
    // RawWalkResponse uses Multipart.parse, so we can rely on TestMultipart
    // to validate multipart parsing works.
}
