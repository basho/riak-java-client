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

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpRequestBase;
import org.junit.Test;

import com.basho.riak.client.http.response.DefaultHttpResponse;
import com.basho.riak.client.util.CharsetUtils;

public class TestDefaultHttpResponse {

    DefaultHttpResponse impl;

    @Test public void status_2xx_300_and_304_success_for_head() {
        HttpHead head = new HttpHead();
        
        for (int i = 200; i < 300; i++) {
            impl = new DefaultHttpResponse(null, null, i, null, null, null,null, head);
            assertTrue(impl.isSuccess());
        }

        impl = new DefaultHttpResponse(null, null, 300, null, null, null, null, head);
        assertTrue(impl.isSuccess());

        impl = new DefaultHttpResponse(null, null, 304, null, null, null, null, head);
        assertTrue(impl.isSuccess());
    }
    @Test public void status_2xx_300_and_304_success_for_get() {
        HttpGet get = new HttpGet();

        for (int i = 200; i < 300; i++) {
            impl = new DefaultHttpResponse(null, null, i, null, null, null, null, get);
            assertTrue(impl.isSuccess());
        }

        impl = new DefaultHttpResponse(null, null, 300, null, null, null, null, get);
        assertTrue(impl.isSuccess());

        impl = new DefaultHttpResponse(null, null, 304, null, null, null, null, get);
        assertTrue(impl.isSuccess());
    }
    @Test public void status_2xx_and_404_success_for_delete() {
        HttpDelete delete = new HttpDelete();

        for (int i = 200; i < 300; i++) {
            impl = new DefaultHttpResponse(null, null, i, null, null, null, null, delete);
            assertTrue(impl.isSuccess());
        }

        impl = new DefaultHttpResponse(null, null, 404, null, null, null, null, delete);
        assertTrue(impl.isSuccess());
    }
    @Test public void status_4xx_is_error() {
        for (int i = 400; i < 500; i++) {
            impl = new DefaultHttpResponse(null, null, i, null, null, null, null, null);
            assertTrue(impl.isError());
        }
    }
    @Test public void status_5xx_is_error() {
        for (int i = 500; i < 600; i++) {
            impl = new DefaultHttpResponse(null, null, i, null, null, null, null, null);
            assertTrue(impl.isError());
        }
    }
    @Test public void status_less_than_100_is_error() {
        for (int i = -1; i < 100; i++) {
            impl = new DefaultHttpResponse(null, null, i, null, null, null, null, null);
            assertTrue(impl.isError());
        }
    }
    @Test public void null_http_method_with_200_is_still_success() {
        impl = new DefaultHttpResponse(null, null, 200, null, null, null, null, null);
        assertTrue(impl.isSuccess());
    }
    @Test public void headers_are_never_null() {
        impl = new DefaultHttpResponse(null, null, 200, null, null, null, null, null);
        assertNotNull(impl.getHttpHeaders());
    }
    
    @Test public void close_closes_underlying_http_connection() throws IOException {
        HttpRequestBase mockHttpMethod = mock(HttpRequestBase.class);
        HttpResponse mockResponse = mock(HttpResponse.class);
        HttpEntity mockEntity = mock(HttpEntity.class);
        InputStream is = new ByteArrayInputStream(CharsetUtils.utf8StringToBytes("a horse a horse my kingdom for a horse"));
        is = spy(is);

        when(mockResponse.getEntity()).thenReturn(mockEntity);
        when(mockEntity.getContent()).thenReturn(is);
        when(mockEntity.isStreaming()).thenReturn(true);

        impl = new DefaultHttpResponse(null, null, 200, null, null, null, mockResponse, mockHttpMethod);
        impl.close();
        verify(is).close();
    }
}
