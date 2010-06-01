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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;
import org.json.JSONException;
import org.junit.Test;

public class TestHttpResponseDecorator {

    @Test public void delegates_http_response_methods_to_impl() throws JSONException {
        
        final String BUCKET = "bucket";
        final String KEY = "key";
        final byte[] BODY = "body".getBytes();
        final int STATUS_CODE = 1;
        final Map<String, String> HTTP_HEADERS = new HashMap<String, String>();
        final HttpMethod HTTP_METHOD = mock(HttpMethod.class);
        final boolean IS_SUCCESS = true;
        final boolean IS_ERROR = true;
        
        HttpResponse mockHttpResponse = mock(HttpResponse.class);
        when(mockHttpResponse.getBucket()).thenReturn(BUCKET);
        when(mockHttpResponse.getKey()).thenReturn(KEY);
        when(mockHttpResponse.getBody()).thenReturn(BODY);
        when(mockHttpResponse.getStatusCode()).thenReturn(STATUS_CODE);
        when(mockHttpResponse.getHttpHeaders()).thenReturn(HTTP_HEADERS);
        when(mockHttpResponse.getHttpMethod()).thenReturn(HTTP_METHOD);
        when(mockHttpResponse.isSuccess()).thenReturn(IS_SUCCESS);
        when(mockHttpResponse.isError()).thenReturn(IS_ERROR);
        
        HttpResponseDecorator impl = new HttpResponseDecorator(mockHttpResponse);
        
        assertEquals(BUCKET, impl.getBucket());
        assertEquals(KEY, impl.getKey());
        assertEquals(BODY, impl.getBody());
        assertEquals(STATUS_CODE, impl.getStatusCode());
        assertSame(HTTP_HEADERS, impl.getHttpHeaders());
        assertSame(HTTP_METHOD, impl.getHttpMethod());
        assertEquals(IS_SUCCESS, impl.isSuccess());
        assertEquals(IS_ERROR, impl.isError());
        
        verify(mockHttpResponse).getBucket();
        verify(mockHttpResponse).getKey();
//        verify(mockHttpResponse).getBody();
        verify(mockHttpResponse).getStatusCode();
        verify(mockHttpResponse).getHttpHeaders();
        verify(mockHttpResponse).getHttpMethod();
        verify(mockHttpResponse).isSuccess();
        verify(mockHttpResponse).isError();
    }
}
