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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;

import org.json.JSONException;
import org.junit.Test;

import com.basho.riak.client.http.response.BucketResponse;
import com.basho.riak.client.http.response.HttpResponse;
import com.basho.riak.client.util.CharsetUtils;

public class TestBucketResponse {

    final String TEXT_BODY = 
        "{\"props\":" +
            "{\"name\":\"b\"," +
            "\"allow_mult\":false," +
            "\"big_vclock\":50," +
            "\"chash_keyfun\":" +
                "{\"mod\":\"riak_util\"," +
                "\"fun\":\"chash_std_keyfun\"}," +
            "\"linkfun\":" +
                "{\"mod\":\"jiak_object\"," +
                "\"fun\":\"mapreduce_linkfun\"}," +
            "\"n_val\":3," +
            "\"old_vclock\":86400," +
            "\"small_vclock\":10," +
            "\"young_vclock\":20}," +
            "\"keys\":" +
                "[\"j\",\"k\",\"l\"]}";
    final byte[] BODY = CharsetUtils.utf8StringToBytes(TEXT_BODY);
    final InputStream STREAM = new ByteArrayInputStream( 
        (CharsetUtils.utf8StringToBytes("{\"props\":" +
            "{\"name\":\"b\"," +
            "\"allow_mult\":false," +
            "\"big_vclock\":50," +
            "\"chash_keyfun\":" +
                "{\"mod\":\"riak_util\"," +
                "\"fun\":\"chash_std_keyfun\"}," +
            "\"linkfun\":" +
                "{\"mod\":\"jiak_object\"," +
                "\"fun\":\"mapreduce_linkfun\"}," +
            "\"n_val\":3," +
            "\"old_vclock\":86400," +
            "\"small_vclock\":10," +
            "\"young_vclock\":20}}" +
        "{\"keys\":[\"j\"]}{\"keys\":[]}{\"keys\":[]}{\"keys\":[\"k\",\"l\"]}")
        ));

    @Test public void doesnt_throw_on_null_impl() throws JSONException, IOException {
        new BucketResponse(null);
    }

    @Test public void parses_schema_field() throws JSONException, IOException {
        HttpResponse mockHttpResponse = mock(HttpResponse.class);
        when(mockHttpResponse.getBody()).thenReturn(BODY);
        when(mockHttpResponse.getBodyAsString()).thenReturn(TEXT_BODY);
        when(mockHttpResponse.isSuccess()).thenReturn(true);
        
        BucketResponse impl = new BucketResponse(mockHttpResponse);
        
        assertEquals(false, impl.getBucketInfo().getAllowMult());
        assertEquals(3, impl.getBucketInfo().getNVal().intValue());
        assertEquals("riak_util:chash_std_keyfun", impl.getBucketInfo().getCHashFun());
        assertEquals("jiak_object:mapreduce_linkfun", impl.getBucketInfo().getLinkFun());
    }

    @Test public void parses_keys_field() throws Exception {
        HttpResponse mockHttpResponse = mock(HttpResponse.class);
        when(mockHttpResponse.getBody()).thenReturn(BODY);
        when(mockHttpResponse.getBodyAsString()).thenReturn(TEXT_BODY);
        when(mockHttpResponse.isSuccess()).thenReturn(true);
        
        BucketResponse impl = new BucketResponse(mockHttpResponse);
        Collection<String> keys = impl.getBucketInfo().getKeys();
        
        assertEquals(3, keys.size());
        assertTrue(keys.contains("j"));
        assertTrue(keys.contains("k"));
        assertTrue(keys.contains("l"));
    }

    @Test public void parses_streamed_schema_field() throws JSONException, IOException {
        HttpResponse mockHttpResponse = mock(HttpResponse.class);
        when(mockHttpResponse.getStream()).thenReturn(STREAM);
        when(mockHttpResponse.isStreamed()).thenReturn(true);
        when(mockHttpResponse.isSuccess()).thenReturn(true);
        
        BucketResponse impl = new BucketResponse(mockHttpResponse);
        
        assertEquals(false, impl.getBucketInfo().getAllowMult());
        assertEquals(3, impl.getBucketInfo().getNVal().intValue());
        assertEquals("riak_util:chash_std_keyfun", impl.getBucketInfo().getCHashFun());
        assertEquals("jiak_object:mapreduce_linkfun", impl.getBucketInfo().getLinkFun());
    }

    @Test public void parses_streamed_keys() throws Exception {
        HttpResponse mockHttpResponse = mock(HttpResponse.class);
        when(mockHttpResponse.getStream()).thenReturn(STREAM);
        when(mockHttpResponse.isStreamed()).thenReturn(true);
        when(mockHttpResponse.isSuccess()).thenReturn(true);
        
        BucketResponse impl = new BucketResponse(mockHttpResponse);
        Collection<String> keys = impl.getBucketInfo().getKeys();

        Iterator<String> key = keys.iterator();
        assertEquals("j", key.next());
        assertEquals("k", key.next());
        assertEquals("l", key.next());
    }
    
    @Test public void returns_empty_keys_list_if_keys_element_not_in_response() throws JSONException, IOException {
        final byte[] body = CharsetUtils.utf8StringToBytes("{\"props\": {\"name\":\"b\"}}");
        HttpResponse mockHttpResponse = mock(HttpResponse.class);
        when(mockHttpResponse.getBody()).thenReturn(body);
        when(mockHttpResponse.getBodyAsString()).thenReturn(CharsetUtils.asUTF8String(body));
        when(mockHttpResponse.isSuccess()).thenReturn(true);
        
        BucketResponse impl = new BucketResponse(mockHttpResponse);
        
        assertNotNull(impl.getBucketInfo().getKeys());
        assertTrue(impl.getBucketInfo().getKeys().isEmpty());
    }
}
