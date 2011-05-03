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
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.basho.riak.client.http.RiakClient;
import com.basho.riak.client.http.response.HttpResponse;
import com.basho.riak.client.http.response.RiakResponseRuntimeException;
import com.basho.riak.client.http.response.WalkResponse;

public class TestWalkResponse {

    final String BUCKET = "bucket";
    final String KEY = "key";
    final Map<String, String> HEADERS = new HashMap<String, String>();

    @Mock HttpResponse mockHttpResponse;
    @Mock RiakClient mockRiakClient;

    @Before public void setup() {
        HEADERS.put("Content-Type".toLowerCase(), "multipart/mixed; boundary=BCVLGEKnH0gY7KsH5nW3xnzhYbU");

        MockitoAnnotations.initMocks(this);

        when(mockHttpResponse.getBucket()).thenReturn(BUCKET);
        when(mockHttpResponse.getKey()).thenReturn(KEY);
        when(mockHttpResponse.getHttpHeaders()).thenReturn(HEADERS);
    }

    @Test public void doesnt_throw_on_null_impl() throws JSONException {
        new WalkResponse(null, null);
    }

    @Test public void returns_empty_list_on_no_content() {
        when(mockHttpResponse.getBody()).thenReturn("".getBytes());
        when(mockHttpResponse.isSuccess()).thenReturn(true);

        WalkResponse impl = new WalkResponse(mockHttpResponse, mockRiakClient);

        assertFalse(impl.hasSteps());
        assertEquals(0, impl.getSteps().size());
    }

    @Test public void parses_walk_steps() {
        final String BODY = "\r\n" + "--BCVLGEKnH0gY7KsH5nW3xnzhYbU\r\n"
                            + "Content-Type: multipart/mixed; boundary=7Ymillu08Tqzwb9Cm6Bs8OewFd5\r\n" 
                            + "\r\n"
                            + "--7Ymillu08Tqzwb9Cm6Bs8OewFd5\r\n"
                            + "X-Riak-Vclock: vclock1\r\n"
                            + "Location: /riak/b/k1\r\n" 
                            + "\r\n" 
                            + "foo\r\n"
                            + "--7Ymillu08Tqzwb9Cm6Bs8OewFd5\r\n" 
                            + "X-Riak-Vclock: vclock2\r\n"
                            + "Location: /riak/b/k2\r\n" 
                            + "\r\n"
                            + "bar\r\n"
                            + "--7Ymillu08Tqzwb9Cm6Bs8OewFd5--\r\n"
                            + "\r\n"
                            + "--BCVLGEKnH0gY7KsH5nW3xnzhYbU--\r\n";

        when(mockHttpResponse.getBody()).thenReturn(BODY.getBytes());
        when(mockHttpResponse.getBodyAsString()).thenReturn(BODY);
        when(mockHttpResponse.isSuccess()).thenReturn(true);

        WalkResponse impl = new WalkResponse(mockHttpResponse, mockRiakClient);
        assertTrue(impl.hasSteps());
        assertEquals(1, impl.getSteps().size());

        assertSame(mockRiakClient, impl.getSteps().get(0).get(0).getRiakClient());
        assertEquals("b", impl.getSteps().get(0).get(0).getBucket());
        assertEquals("k1", impl.getSteps().get(0).get(0).getKey());
        assertEquals("foo", impl.getSteps().get(0).get(0).getValue());
        assertEquals("vclock1", impl.getSteps().get(0).get(0).getVclock());

        assertSame(mockRiakClient, impl.getSteps().get(0).get(1).getRiakClient());
        assertEquals("b", impl.getSteps().get(0).get(1).getBucket());
        assertEquals("k2", impl.getSteps().get(0).get(1).getKey());
        assertEquals("bar", impl.getSteps().get(0).get(1).getValue());
        assertEquals("vclock2", impl.getSteps().get(0).get(1).getVclock());
    }

    @Test(expected = RiakResponseRuntimeException.class) public void throws_on_invalid_subpart_content_type() {
        final String BODY = "\r\n" 
            + "--BCVLGEKnH0gY7KsH5nW3xnzhYbU\r\n"
            + "Content-Type: text/plain\r\n"
            + "\r\n"
            + "--7Ymillu08Tqzwb9Cm6Bs8OewFd5\r\n" 
            + "\r\n"
            + "--7Ymillu08Tqzwb9Cm6Bs8OewFd5--\r\n" 
            + "\r\n"
            + "--BCVLGEKnH0gY7KsH5nW3xnzhYbU--\r\n";

        when(mockHttpResponse.getBody()).thenReturn(BODY.getBytes());
        when(mockHttpResponse.getBodyAsString()).thenReturn(BODY);
        when(mockHttpResponse.isSuccess()).thenReturn(true);

        new WalkResponse(mockHttpResponse, mockRiakClient);
    }

    // WalkResponse uses Multipart.parse, so we can rely on TestMultipart
    // to validate multipart parsing works.
}
