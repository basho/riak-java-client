package com.basho.riak.client.raw;

import static org.junit.Assert.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.basho.riak.client.RiakLink;

public class TestRawUtils {

    @Test public void parses_null_or_empty_link_header() {
        Collection<RiakLink> links;
        
        links = RawUtils.parseLinkHeader(null);
        assertNotNull(links);
        assertTrue(links.isEmpty());
        
        RawUtils.parseLinkHeader("");
        assertNotNull(links);
        assertTrue(links.isEmpty());
    }

    @Test public void parses_links_from_header() {
        // Rely on LinkHeader tests to verify that link parsing actually works for other cases (single link, multiple links, malformed, etc)
        List<RiakLink> links = RawUtils.parseLinkHeader("</link/1>; riaktag=t, </link/2>; riaktag=\"abc\"");
        assertEquals(2, links.size());
        assertEquals(new RiakLink("link", "1", "t"), links.get(0));
        assertEquals(new RiakLink("link", "2", "abc"), links.get(1));
    }
    

    @Test public void parse_usermeta_handles_null_or_empty_header_set() {
        Map<String, String> usermeta;
        
        usermeta = RawUtils.parseUsermeta(null);
        assertNotNull(usermeta);
        assertTrue(usermeta.isEmpty());
        
        usermeta = RawUtils.parseUsermeta(new HashMap<String, String>());
        assertNotNull(usermeta);
        assertTrue(usermeta.isEmpty());
    }

    @Test public void parse_usermeta_returns_correct_headers() {
        @SuppressWarnings("serial") Map<String, String> usermeta = new HashMap<String, String>() {{
            put("h1", "v1");
            put("h2", "v2");
            put("X-Riak-Meta-Test", "v");
        }};
        
        usermeta = RawUtils.parseUsermeta(usermeta);
        assertEquals(1, usermeta.size());
        assertEquals("v", usermeta.get("Test"));
    }
    
    @Test public void parse_usermeta_is_case_insensitive() {
        @SuppressWarnings("serial") Map<String, String> usermeta = new HashMap<String, String>() {{
            put("X-Riak-Meta-Test", "v");
            put("x-riak-meta-Test2", "v2");
        }};
        
        usermeta = RawUtils.parseUsermeta(usermeta);
        assertEquals(2, usermeta.size());
        assertEquals("v", usermeta.get("Test"));
        assertEquals("v2", usermeta.get("Test2"));
    }
    
    @Test public void parse_multipart_handles_null_params() {
        List<RawObject> objects = RawUtils.parseMultipart(null, null, null, null);
        assertNotNull(objects);
    }

    @Test public void parse_multipart_returns_correct_bucket_and_key() {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("content-type", "multipart/mixed; boundary=boundary");
        String body =  
            "\n--boundary\n" +
            "\n" +
            "--boundary--";
        
        List<RawObject> objects = RawUtils.parseMultipart("b", "k", headers, body);
        assertEquals(1, objects.size());
        assertEquals("b", objects.get(0).getBucket());
        assertEquals("k", objects.get(0).getKey());
    }

    @Test public void parse_multipart_returns_all_header_metadata_for_object() {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("content-type", "multipart/mixed; boundary=boundary");
        headers.put("x-riak-vclock", "vclock");
        String body =  
            "\n--boundary\n" +
            "Content-Type: text/plain\n" +
            "Last-Modified: lastmod\n" +
            "Link: </raw/b/l>; riaktag=t\n" +
            "ETag: vtag\n" +
            "X-Riak-Meta-Test: value\n" +
            "\n" +
            "--boundary--";
        
        List<RawObject> objects = RawUtils.parseMultipart("b", "k", headers, body);

        assertEquals(1, objects.get(0).getLinks().size());
        assertEquals(new RiakLink("b", "l", "t"), objects.get(0).getLinks().get(0));
        
        assertEquals(1, objects.get(0).getUsermeta().size());
        assertEquals("value", objects.get(0).getUsermeta().get("test"));
        
        assertEquals("text/plain", objects.get(0).getContentType());
        assertEquals("vclock", objects.get(0).getVclock());
        assertEquals("lastmod", objects.get(0).getLastmod());
        assertEquals("vtag", objects.get(0).getVtag());
    }

    @Test public void parse_multipart_returns_value() {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("content-type", "multipart/mixed; boundary=boundary");
        String body =  
            "\n--boundary\n" +
            "\n" +
            "foo\n" +
            "--boundary--";
        
        List<RawObject> objects = RawUtils.parseMultipart("b", "k", headers, body);
        assertEquals("foo", objects.get(0).getValue());
    }

    // Rely on Multipart tests to verify that multipart parsing actually works
}
