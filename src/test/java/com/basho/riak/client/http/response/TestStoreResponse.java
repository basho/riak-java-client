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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.junit.Test;

import com.basho.riak.client.http.response.FetchResponse;
import com.basho.riak.client.http.response.StoreResponse;

public class TestStoreResponse {

    @Test public void doesnt_throw_on_null_impl() throws JSONException {
        new StoreResponse(null);
    }
    
    @Test public void parses_meta_headers() {
        final FetchResponse mockFetchResponse = mock(FetchResponse.class);
        final Map<String, String> HTTP_HEADERS = new HashMap<String, String>();
        HTTP_HEADERS.put("X-Riak-Vclock".toLowerCase(), "a85hYGBgzGDKBVIsDPKZOzKYEhnzWBlaJyw9wpcFAA==");
        HTTP_HEADERS.put("Last-Modified".toLowerCase(), "Tue, 22 Dec 2009 18:48:37 GMT");
        HTTP_HEADERS.put("ETag".toLowerCase(), "4d5y9wqQK2Do0RK5ezwCJD");

        when(mockFetchResponse.getHttpHeaders()).thenReturn(HTTP_HEADERS);
        when(mockFetchResponse.isSuccess()).thenReturn(true);

        StoreResponse impl = new StoreResponse(mockFetchResponse);

        assertEquals("a85hYGBgzGDKBVIsDPKZOzKYEhnzWBlaJyw9wpcFAA==", impl.getVclock());
        assertEquals("Tue, 22 Dec 2009 18:48:37 GMT", impl.getLastmod());
        assertEquals("4d5y9wqQK2Do0RK5ezwCJD", impl.getVtag());
    }

    @Test public void delegates_multiimpl_to_bodyresponse() {
        final FetchResponse mockFetchResponse = mock(FetchResponse.class);
        StoreResponse impl = new StoreResponse(mockFetchResponse);

        impl.hasObject();
        impl.getObject();
        impl.hasSiblings();
        impl.getSiblings();

        verify(mockFetchResponse, times(1)).hasObject();
        verify(mockFetchResponse, times(1)).getObject();
        verify(mockFetchResponse, times(1)).hasSiblings();
        verify(mockFetchResponse, times(1)).getSiblings();

    }
}
