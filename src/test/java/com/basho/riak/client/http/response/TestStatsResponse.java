/*
 * Copyright 2012 roach.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.http.response;

import static org.mockito.Mockito.*;

import com.basho.riak.client.http.response.StatsResponse;
import com.basho.riak.client.http.response.HttpResponse;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author roach
 */
public class TestStatsResponse
{
    @Test public void doesnt_throw_on_valid_json() throws JSONException
    {
        final HttpResponse mockHttpResponse = mock(HttpResponse.class);
        final String json = "{\"key\":\"value\"}";
        when(mockHttpResponse.getBodyAsString()).thenReturn(json);
        when(mockHttpResponse.isSuccess()).thenReturn(true);
        
        StatsResponse resp = new StatsResponse(mockHttpResponse);
    }
    
    @Test public void throws_on_invalid_json() 
    {
        final HttpResponse mockHttpResponse = mock(HttpResponse.class);
        final String json = "{key\":\"value\"}";
        when(mockHttpResponse.getBodyAsString()).thenReturn(json);
        when(mockHttpResponse.isSuccess()).thenReturn(true);
        
        try
        {
            StatsResponse resp = new StatsResponse(mockHttpResponse);
            fail("Expected JSONException when given invalid JSON");
        }
        catch (JSONException ex)
        {
            // no-op - this is a success
        }
    }
    
    @Test public void throws_on_failed_http_response() throws JSONException
    {
        final HttpResponse mockHttpResponse = mock(HttpResponse.class);
        final String json = "{key\":\"value\"}";
        when(mockHttpResponse.getBodyAsString()).thenReturn(json);
        when(mockHttpResponse.isSuccess()).thenReturn(false);
        
        try
        {
            StatsResponse resp = new StatsResponse(mockHttpResponse);
            fail("Expected RiakResponseRuntimeException when given failed HttpResponse");
        }
        catch (RiakResponseRuntimeException ex)
        {
            // no-op - this is a success
        }
    }
    
    
    @Test public void creates_JSONObject() throws JSONException
    {
        final HttpResponse mockHttpResponse = mock(HttpResponse.class);
        final String json = "{\"key\":\"value\"}";
        when(mockHttpResponse.getBodyAsString()).thenReturn(json);
        when(mockHttpResponse.isSuccess()).thenReturn(true);
        
        StatsResponse resp = new StatsResponse(mockHttpResponse);
        
        JSONObject parsedJson = resp.getStats();
        assertTrue(parsedJson.has("key"));
        assertEquals("value", parsedJson.get("key").toString());
        
    }
    
}
