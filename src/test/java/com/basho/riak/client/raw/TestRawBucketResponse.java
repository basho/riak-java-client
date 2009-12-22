package com.basho.riak.client.raw;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;
import org.json.JSONException;
import org.junit.Test;

import com.basho.riak.client.response.HttpResponse;

public class TestRawBucketResponse {

    final String BODY = 
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

    @Test public void delegates_http_response_methods_to_impl() throws JSONException {
        final String BUCKET = "bucket";
        final String KEY = "key";
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
        
        RawBucketResponse impl = new RawBucketResponse(mockHttpResponse);
        
        assertEquals(BUCKET, impl.getBucket());
        assertEquals(KEY, impl.getKey());
        assertEquals(STATUS_CODE, impl.getStatusCode());
        assertSame(HTTP_HEADERS, impl.getHttpHeaders());
        assertSame(HTTP_METHOD, impl.getHttpMethod());
        assertEquals(IS_SUCCESS, impl.isSuccess());
        assertEquals(IS_ERROR, impl.isError());
        
        verify(mockHttpResponse).getBucket();
        verify(mockHttpResponse).getKey();
        verify(mockHttpResponse, atLeastOnce()).getBody(); // also called by constructor
        verify(mockHttpResponse).getStatusCode();
        verify(mockHttpResponse).getHttpHeaders();
        verify(mockHttpResponse).getHttpMethod();
        verify(mockHttpResponse, atLeastOnce()).isSuccess(); // also called by constructor
        verify(mockHttpResponse).isError();
    }

    @Test public void doesnt_throw_on_null_impl() throws JSONException {
        new RawBucketResponse(null);
    }

    @Test public void parses_schema_field() throws JSONException {
        HttpResponse mockHttpResponse = mock(HttpResponse.class);
        when(mockHttpResponse.getBody()).thenReturn(BODY);
        when(mockHttpResponse.isSuccess()).thenReturn(true);
        
        RawBucketResponse impl = new RawBucketResponse(mockHttpResponse);
        
        assertEquals(false, impl.getBucketInfo().getAllowMult());
        assertEquals(3, impl.getBucketInfo().getNVal().intValue());
        assertEquals("riak_util:chash_std_keyfun", impl.getBucketInfo().getCHashFun());
        assertEquals("jiak_object:mapreduce_linkfun", impl.getBucketInfo().getLinkFun());
    }

    @Test public void parses_keys_field() throws Exception {
        HttpResponse mockHttpResponse = mock(HttpResponse.class);
        when(mockHttpResponse.getBody()).thenReturn(BODY);
        when(mockHttpResponse.isSuccess()).thenReturn(true);
        
        RawBucketResponse impl = new RawBucketResponse(mockHttpResponse);
        Collection<String> keys = impl.getBucketInfo().getKeys();
        
        assertEquals(3, keys.size());
        assertTrue(keys.contains("j"));
        assertTrue(keys.contains("k"));
        assertTrue(keys.contains("l"));
    }
}
