package com.basho.riak.client.jiak;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import com.basho.riak.client.RiakLink;
import com.basho.riak.client.util.Constants;

public class TestJiakObject {
    
    JiakObject impl;

    @Before public void setup() {
        impl = new JiakObject("b", "k");
    }
    
    @Test public void constructor_from_json_parses_all_fields() throws JSONException {
        final String json = 
            "{\"object\":{\"value1\":1}," +
            "\"vclock\":\"a85hYGBgzGDKBVIsrBVloRlMiYx5rAyzr2w/wpcFAA==\"," +
            "\"lastmod\":\"Tue, 05 Jan 2010 15:19:55 GMT\"," +
            "\"vtag\":\"UFPmrwpkMVfuKmoTqZAd3\"," +
            "\"bucket\":\"b\"," +
            "\"key\":\"k\"," +
            "\"links\":[[\"b\",\"j\",\"t\"]]}";
        impl = new JiakObject(new JSONObject(json));
        
        assertEquals("{\"value1\":1}", impl.getValue());
        assertEquals("a85hYGBgzGDKBVIsrBVloRlMiYx5rAyzr2w/wpcFAA==", impl.getVclock());
        assertEquals("Tue, 05 Jan 2010 15:19:55 GMT", impl.getLastmod());
        assertEquals("UFPmrwpkMVfuKmoTqZAd3", impl.getVtag());
        assertEquals("b", impl.getBucket());
        assertEquals("k", impl.getKey());
        assertEquals(1, impl.getLinks().size());
        assertEquals("[\"b\",\"j\",\"t\"]", impl.getLinksAsJSON().getString(0));
    }
    
    @Test public void content_type_is_json() {
        assertEquals("application/json", impl.getContentType());
    }
    
    @Test public void links_never_null() {
        impl = new JiakObject("b", "k", null, null, null, null, null, null);
        assertNotNull(impl.getLinks());
        
        impl.setLinks((List<RiakLink>) null);
        assertNotNull(impl.getLinks());

        impl.copyData(new JiakObject(null, null));
        assertNotNull(impl.getLinks());
    }


    @Test public void usermeta_never_null() {
        impl = new JiakObject("b", "k", null, null, null, null, null, null);
        assertNotNull(impl.getUsermeta());
        
        impl.setUsermeta((Map<String, String>) null);
        assertNotNull(impl.getUsermeta());

        impl.copyData(new JiakObject(null, null));
        assertNotNull(impl.getUsermeta());
    }

    @Test public void copyData_does_deep_copy() throws JSONException {
        final JSONObject value = new JSONObject("{\"v\":1}");
        final List<RiakLink> links = new ArrayList<RiakLink>();
        final Map<String, String> usermeta = new HashMap<String, String>();
        final String vclock = "vclock";
        final String lastmod = "lastmod";
        final String vtag = "vtag";
        final RiakLink link = new RiakLink("b", "l", "t"); 
        links.add(link);
     
        impl.copyData(new JiakObject("b", "k2", value, links, usermeta, vclock, lastmod, vtag));
        
        assertEquals("b", impl.getBucket());
        assertEquals("k", impl.getKey());
        assertEquals(value.toString(), impl.getValue());
        assertEquals(links, impl.getLinks());
        assertEquals(usermeta, impl.getUsermeta());
        assertEquals(vclock, impl.getVclock());
        assertEquals(lastmod, impl.getLastmod());
        assertEquals(vtag, impl.getVtag());

        assertNotSame(value, impl.getValueAsJSON());
        assertNotSame(links, impl.getLinks());
        assertNotSame(link, impl.getLinks().get(0));
        assertNotSame(usermeta, impl.getUsermeta());
    }

    @Test public void copyData_copies_null_data() throws JSONException {
        final JSONObject value = new JSONObject("{\"v\":1}");
        final List<RiakLink> links = new ArrayList<RiakLink>();
        final Map<String, String> usermeta = new HashMap<String, String>();
        final String vclock = "vclock";
        final String lastmod = "lastmod";
        final String vtag = "vtag";
        final RiakLink link = new RiakLink("b", "l", "t"); 
        links.add(link);
        
        impl = new JiakObject("b", "k", value, links, usermeta, vclock, lastmod, vtag);
        impl.copyData(new JiakObject(null, null));
        
        assertEquals("b", impl.getBucket());
        assertEquals("k", impl.getKey());
        assertFalse(impl.getValueAsJSON().keys().hasNext());
        assertEquals(0, impl.getLinks().size());
        assertEquals(0, impl.getUsermeta().size());
        assertNull(impl.getVclock());
        assertNull(impl.getLastmod());
        assertNull(impl.getVtag());
    }
    
    @Test(expected=IllegalArgumentException.class) public void set_value_throws_on_illegal_json() {
        impl.setValue("illegal json");
    }
    
    @Test public void get_usermeta_as_json_returns_all_usermeta() throws JSONException {
        @SuppressWarnings("serial") final Map<String, String> usermeta = new HashMap<String, String>() {{
            put("k", "v");
            put("k2", "v2");
            put("k3", "v3");
        }};
        
        impl.setUsermeta(usermeta);
        JSONObject jsonUsermeta = impl.getUsermetaAsJSON();
        for (String field : usermeta.keySet()) {
            assertTrue(jsonUsermeta.has(field));
            assertEquals(usermeta.get(field), jsonUsermeta.getString(field));
        }
    }
    
    @Test public void get_links_as_json_returns_all_links_in_order() throws JSONException {
        @SuppressWarnings("serial") final List<RiakLink> links = new ArrayList<RiakLink>() {{
            add(new RiakLink("b", "k", "t"));
            add(new RiakLink("b2", "k2", "t2"));
            add(new RiakLink("b3", "k3", "t3"));
        }};
        
        impl.setLinks(links);
        JSONArray jsonLinks = impl.getLinksAsJSON();
        int i = 0;
        for (RiakLink link : links) {
            assertEquals(link.getBucket(), jsonLinks.getJSONArray(i).getString(0));
            assertEquals(link.getKey(), jsonLinks.getJSONArray(i).getString(1));
            assertEquals(link.getTag(), jsonLinks.getJSONArray(i).getString(2));
            i++;
        }
    }
    
    @Test public void write_to_http_method_delegates_to_to_json_object() {
        impl = spy(impl);
        impl.writeToHttpMethod(mock(EntityEnclosingMethod.class));
        verify(impl).toJSONObject();
    }
    
    @Test public void to_json_object_adds_links() throws JSONException {
        @SuppressWarnings("serial") final List<RiakLink> links = new ArrayList<RiakLink>() {{
            add(new RiakLink("b", "k", "t"));
            add(new RiakLink("b2", "k2", "t2"));
            add(new RiakLink("b3", "k3", "t3"));
        }};
        impl.setLinks(links);

        JSONArray jsonLinks = impl.getLinksAsJSON();
        assertTrue(impl.toJSONObject().has(Constants.JIAK_FL_LINKS));
        assertEquals(jsonLinks.toString(), impl.toJSONObject().getJSONArray(Constants.JIAK_FL_LINKS).toString());
    }
    
    @Test public void to_json_object_adds_empty_links_array_if_none() {
        assertTrue(impl.toJSONObject().has(Constants.JIAK_FL_LINKS));
    }
    
    @Test public void to_json_object_adds_usermeta() throws JSONException {
        @SuppressWarnings("serial") final Map<String, String> usermeta = new HashMap<String, String>() {{
            put("k", "v");
        }};
        
        impl.setUsermeta(usermeta);

        assertTrue(impl.toJSONObject().getJSONObject(Constants.JIAK_FL_VALUE).has(Constants.JIAK_FL_USERMETA));
        assertEquals("v", impl.toJSONObject().getJSONObject(Constants.JIAK_FL_VALUE).getJSONObject(Constants.JIAK_FL_USERMETA).getString("k"));
    }

    @Test public void to_json_object_adds_usermeta_even_if_no_value() throws JSONException {
        @SuppressWarnings("serial") final Map<String, String> usermeta = new HashMap<String, String>() {{
            put("k", "v");
        }};
        
        impl.setUsermeta(usermeta);
        impl.setValue((JSONObject) null);

        assertTrue(impl.toJSONObject().getJSONObject(Constants.JIAK_FL_VALUE).has(Constants.JIAK_FL_USERMETA));
    }

    @Test public void to_json_object_overwrites_usermeta_in_value() throws JSONException {
        @SuppressWarnings("serial") final Map<String, String> usermeta = new HashMap<String, String>() {{
            put("k", "v");
        }};
        
        impl.setUsermeta(usermeta);
        impl.setValue(new JSONObject("{\"" + Constants.JIAK_FL_VALUE + "\": {\"" + Constants.JIAK_FL_USERMETA + "\": {\"a\": \"b\"}}}"));

        assertTrue(impl.toJSONObject().getJSONObject(Constants.JIAK_FL_VALUE).has(Constants.JIAK_FL_USERMETA));
        assertFalse(impl.toJSONObject().getJSONObject(Constants.JIAK_FL_VALUE).getJSONObject(Constants.JIAK_FL_USERMETA).has("a"));
        assertEquals("v", impl.toJSONObject().getJSONObject(Constants.JIAK_FL_VALUE).getJSONObject(Constants.JIAK_FL_USERMETA).getString("k"));
    }

    @Test public void to_json_object_doesnt_adds_usermeta_if_none() throws JSONException {
        impl.setUsermeta(null);
        assertFalse(impl.toJSONObject().getJSONObject(Constants.JIAK_FL_VALUE).has(Constants.JIAK_FL_USERMETA));
    }

    @Test public void to_json_object_always_adds_bucket_key_value_and_links_even_if_null() {
        assertTrue(impl.toJSONObject().has(Constants.JIAK_FL_BUCKET));
        assertTrue(impl.toJSONObject().has(Constants.JIAK_FL_KEY));
        assertTrue(impl.toJSONObject().has(Constants.JIAK_FL_VALUE));
        assertTrue(impl.toJSONObject().has(Constants.JIAK_FL_LINKS));
    }

    @Test public void to_json_object_sets_vclock_lastmod_and_vtag_only_when_not_null() {
        assertFalse(impl.toJSONObject().has(Constants.JIAK_FL_VCLOCK));
        assertFalse(impl.toJSONObject().has(Constants.JIAK_FL_LAST_MODIFIED));
        assertFalse(impl.toJSONObject().has(Constants.JIAK_FL_VTAG));

        impl = new JiakObject("b", "k", null, null, null, "vclock", "lastmod", "vtag");
        
        assertEquals("vclock", impl.toJSONObject().optString(Constants.JIAK_FL_VCLOCK));
        assertEquals("lastmod", impl.toJSONObject().optString(Constants.JIAK_FL_LAST_MODIFIED));
        assertEquals("vtag", impl.toJSONObject().optString(Constants.JIAK_FL_VTAG));
    }
}
