package com.basho.riak.client.jiak;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Collection;
import java.util.List;

import org.json.JSONException;
import org.junit.Test;

import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.util.ClientUtils;
import com.basho.riak.client.util.Constants;

public class TestJiakBucketResponse {

    final String BODY = 
        "{\"schema\":" +
            "{\"allowed_fields\":[\"f1\",\"f2\"]," +
            "\"required_fields\":[\"f1\"]," +
            "\"read_mask\":\"*\"," +
            "\"write_mask\":[\"f2\"]}," +
        "\"keys\":[\"j\", \"k\", \"l\"]}";

    @Test public void doesnt_throw_on_null_impl() throws JSONException {
        new JiakBucketResponse(null);
    }

    @Test public void parses_schema_field() throws JSONException {
        HttpResponse mockHttpResponse = mock(HttpResponse.class);
        when(mockHttpResponse.getBody()).thenReturn(BODY);
        when(mockHttpResponse.isSuccess()).thenReturn(true);
        
        JiakBucketResponse impl = new JiakBucketResponse(mockHttpResponse);
        
        List<String> allowedFields = ClientUtils.jsonArrayAsList(impl.getBucketInfo().getSchema().getJSONArray(Constants.JIAK_FL_SCHEMA_ALLOWED_FIELDS));
        List<String> requiredFields = ClientUtils.jsonArrayAsList(impl.getBucketInfo().getSchema().getJSONArray(Constants.JIAK_FL_SCHEMA_REQUIRED_FIELDS));
        String readMask = impl.getBucketInfo().getSchema().getString(Constants.JIAK_FL_SCHEMA_READ_MASK);
        List<String> writeMask = ClientUtils.jsonArrayAsList(impl.getBucketInfo().getSchema().getJSONArray(Constants.JIAK_FL_SCHEMA_WRITE_MASK));

        assertTrue(allowedFields.contains("f1"));
        assertTrue(allowedFields.contains("f2"));
        assertTrue(requiredFields.contains("f1"));
        assertFalse(requiredFields.contains("f2"));
        assertEquals("*", readMask);
        assertFalse(writeMask.contains("f1"));
        assertTrue(writeMask.contains("f2"));
    }
    
    @Test public void parses_keys_field() throws Exception {
        HttpResponse mockHttpResponse = mock(HttpResponse.class);
        when(mockHttpResponse.getBody()).thenReturn(BODY);
        when(mockHttpResponse.isSuccess()).thenReturn(true);
        
        JiakBucketResponse impl = new JiakBucketResponse(mockHttpResponse);
        Collection<String> keys = impl.getBucketInfo().getKeys();
        
        assertEquals(3, keys.size());
        assertTrue(keys.contains("j"));
        assertTrue(keys.contains("k"));
        assertTrue(keys.contains("l"));
    }

}
