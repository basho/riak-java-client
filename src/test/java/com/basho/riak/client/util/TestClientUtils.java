package com.basho.riak.client.util;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpClient;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import com.basho.riak.client.RiakConfig;

public class TestClientUtils {

    RiakConfig config = new RiakConfig();

    @Test public void newHttpClient_uses_configs_http_client_if_exists() {
        HttpClient httpClient = new HttpClient();
        config.setHttpClient(httpClient);
        assertSame(httpClient, ClientUtils.newHttpClient(config));
    }

    @Test public void newHttpClient_sets_max_total_connections() {
        final int maxConnections = 11;
        config.setMaxConnections(maxConnections );
        
        HttpClient httpClient = ClientUtils.newHttpClient(config);
        
        assertEquals(maxConnections, httpClient.getHttpConnectionManager().getParams().getMaxTotalConnections());
    }
    
    @Test public void newHttpClient_sets_connection_timeout() {
        final long timeout = 11;
        config.setTimeout(timeout);
        
        HttpClient httpClient = ClientUtils.newHttpClient(config);
        
        assertEquals(timeout, httpClient.getParams().getConnectionManagerTimeout());
    }
    
    @Test public void makeURI_url_encodes_bucket() {
        String url = ClientUtils.makeURI(config, "/");
        assertTrue("Expected bucket to be encoded as %2F in URL" + url, url.endsWith("/%2F"));
        
    }
    
    @Test public void makeURI_url_encodes_key() {
        String url = ClientUtils.makeURI(config, "b", "/");
        assertTrue("Expected key to be encoded as %2F in URL: " + url, url.endsWith("/b/%2F"));
    }
    
    @Test public void makeURI_prepends_slash_to_extra_if_extra_is_a_path_component() {
        String url = ClientUtils.makeURI(config, "b", "k", "path_component");
        assertTrue("Expected a slash before path_component in URL: " + url, url.endsWith("/path_component"));
    }
    
    @Test public void getPathFromURL_masks_full_url() {
        String path = ClientUtils.getPathFromUrl("http://host.com:10000/path/to/object");
        assertEquals("/path/to/object", path);
    }
    
    @Test public void getPathFromURL_masks_portless_url() {
        String path = ClientUtils.getPathFromUrl("http://host.com/path/to/object");
        assertEquals("/path/to/object", path);
    }

    @Test public void getPathFromURL_masks_schemeless_url() {
        String path = ClientUtils.getPathFromUrl("host.com:10000/path/to/object");
        assertEquals("/path/to/object", path);
    }

    @Test public void getPathFromURL_returns_path_in_path_only_url() {
        String path = ClientUtils.getPathFromUrl("/path/to/object");
        assertEquals("/path/to/object", path);
    }
    
    @Test public void getPathFromURL_handles_empty_url() {
        String path = ClientUtils.getPathFromUrl("");
        assertEquals("", path);
    }

    @Test public void getPathFromURL_handles_null() {
        String path = ClientUtils.getPathFromUrl(null);
        assertNull(path);
    }

    @Test public void unquote_string_removes_surrounding_quotes() {
        String s = ClientUtils.unquoteString("\"string\"");
        assertFalse("Starting quote should be removed", s.startsWith("\""));
        assertFalse("Trailing quote should be removed", s.endsWith("\""));
    }

    @Test public void unquote_string_unescapes_blackslash_escaped_chars() {
        String s = ClientUtils.unquoteString("\"\\\\\\a\\n\"");
        assertEquals("\\an", s);
    }

    @Test public void unquote_string_handles_trailing_blackslash() {
        String s = ClientUtils.unquoteString("\"\\\\\\a\\n\\\"");
        assertEquals("\\an\\", s);
    }

    @Test public void unquote_string_handles_unquoted_string() {
        String s = ClientUtils.unquoteString("\\\\\\a\\n");
        assertEquals("\\an", s);
    }
    
    @Test public void json_opt_string_handles_any_type_and_nulls_without_throwing() throws JSONException {
        // optString() is used by jsonObjectAsMap and jsonArrayAsList with the assumption that
        // it won't throw. Just making sure...
        JSONObject jsonObject = new JSONObject();
        JSONArray jsonArray = new JSONArray();
        
        assertEquals("", jsonObject.optString("k"));
        assertEquals("", jsonArray.optString(0));

        jsonObject.put("k", new JSONArray());
        jsonArray.put(0, new JSONArray());
        assertEquals("[]", jsonObject.optString("k"));
        assertEquals("[]", jsonArray.optString(0));

        jsonObject.put("k", new JSONObject());
        jsonArray.put(0, new JSONObject());
        assertEquals("{}", jsonObject.optString("k"));
        assertEquals("{}", jsonArray.optString(0));

        jsonObject.put("k", 12);
        jsonArray.put(0, 12);
        assertEquals("12", jsonObject.optString("k"));
        assertEquals("12", jsonArray.optString(0));

        jsonObject.put("k", true);
        jsonArray.put(0, true);
        assertEquals("true", jsonObject.optString("k"));
        assertEquals("true", jsonArray.optString(0));
    }
    
    @Test public void json_object_as_map_puts_all_fields_using_opt_string() throws JSONException {
        JSONObject json = spy(new JSONObject().put("k1","v1").put("k2","v2").put("k3","v3"));
        Map<String, String> map = ClientUtils.jsonObjectAsMap(json);
        
        verify(json).optString("k1");
        verify(json).optString("k2");
        verify(json).optString("k3");
        
        assertEquals("v1", map.get("k1"));
        assertEquals("v2", map.get("k2"));
        assertEquals("v3", map.get("k3"));
    }
    
    @Test public void json_array_as_list_adds_all_elements_using_opt_string() {
        JSONArray json = spy(new JSONArray().put("v1").put("v2").put("v3"));
        List<String> list = ClientUtils.jsonArrayAsList(json);
        
        verify(json).optString(0);
        verify(json).optString(1);
        verify(json).optString(2);
        
        assertEquals("v1", list.get(0));
        assertEquals("v2", list.get(1));
        assertEquals("v3", list.get(2));
    }
    
}
